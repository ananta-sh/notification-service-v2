package com.notificationservice.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notificationservice.entity.NotificationEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The OutboxPoller is the engine of the Transactional Outbox Pattern.
 *
 * Runs every ${outbox.poll.interval-ms} milliseconds (default: 2s).
 * Reads up to 100 unpublished outbox events, publishes each to Kafka,
 * then marks them as published.
 *
 * Crash safety:
 *   - If the app crashes mid-poll, unpublished rows remain published=false
 *   - On restart, the poller picks them up again → at-least-once delivery
 *   - The Kafka consumer's idempotency check (DB unique constraint) handles
 *     any duplicates that might result from a crash-then-retry
 *
 * Interview talking point:
 *   "Why not just publish to Kafka directly in the service method?"
 *   → Because DB and Kafka are two separate systems. You cannot span a
 *     single ACID transaction across both. The outbox pattern uses the DB
 *     as the source of truth, and the poller as an eventually-consistent
 *     bridge to Kafka.
 */
@Component
@ConditionalOnProperty(name = "outbox.poll.enabled", havingValue = "true", matchIfMissing = true)
@Slf4j
public class OutboxPoller {

    private final OutboxClaimService outboxClaimService;
    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // Metrics
    private final Counter publishedCounter;
    private final Counter failedCounter;

    public OutboxPoller(OutboxClaimService outboxClaimService,
                        KafkaTemplate<String, NotificationEvent> kafkaTemplate,
                        ObjectMapper objectMapper,
                        MeterRegistry meterRegistry) {
        this.outboxClaimService = outboxClaimService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.publishedCounter = Counter.builder("outbox.published.total")
                .description("Total outbox events successfully published to Kafka")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("outbox.publish.failed.total")
                .description("Total outbox events that failed to publish")
                .register(meterRegistry);
    }

    /**
     * Polls the outbox table every 2 seconds.
     * fixedDelayString means: wait 2s AFTER the previous execution finishes,
     * preventing overlapping polls if Kafka is slow.
     */
    @Scheduled(fixedDelayString = "${outbox.poll.interval-ms:2000}")
    public void pollAndPublish() {
        List<OutboxClaim> pending = outboxClaimService.claimBatch();

        if (pending.isEmpty()) {
            return; // nothing to do, skip noisy log
        }

        log.debug("[OutboxPoller] Found {} unpublished event(s) to publish", pending.size());

        for (OutboxClaim outboxEvent : pending) {
            try {
                if (!outboxClaimService.renew(outboxEvent)) {
                    log.warn("Outbox claim was taken over before publish | eventId={}", outboxEvent.eventId());
                    continue;
                }
                // Deserialize stored JSON payload back to NotificationEvent
                NotificationEvent event = objectMapper.readValue(
                        outboxEvent.payload(), NotificationEvent.class);

                // Wait for Kafka's broker acknowledgement before the next poll can
                // select this row again. A crash after this point can still replay it.
                var result = kafkaTemplate.send(
                        outboxEvent.topic(), outboxEvent.messageKey(), event).get();
                if (!outboxClaimService.markPublished(outboxEvent)) {
                    log.warn("Outbox claim expired before publish confirmation | eventId={}", outboxEvent.eventId());
                    continue;
                }
                log.info("[OutboxPoller] ✓ Published eventId={} → topic={} | partition={} offset={}",
                        outboxEvent.eventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                publishedCounter.increment();

            } catch (Exception e) {
                log.error("[OutboxPoller] Failed to process outbox row id={} | error={}",
                        outboxEvent.id(), e.getMessage());
                outboxClaimService.releaseAfterFailure(outboxEvent);
                failedCounter.increment();
            }
        }
    }
}
