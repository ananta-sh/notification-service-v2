package com.notificationservice.consumer;

import com.notificationservice.entity.NotificationEvent;
import com.notificationservice.exception.DeliveryException;
import com.notificationservice.metrics.NotificationMetrics;
import com.notificationservice.service.ChannelRateLimiterService;
import com.notificationservice.service.NotificationDeliveryProvider;
import com.notificationservice.service.NotificationService;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class EmailConsumer {

    private final NotificationService notificationService;
    private final NotificationDeliveryProvider deliveryProvider;
    private final ChannelRateLimiterService rateLimiter;
    private final NotificationMetrics metrics;

    private static final String CHANNEL = "EMAIL";

    @KafkaListener(
        topics = "${kafka.topics.email}",
        groupId = "notification-email-group",
        concurrency = "3",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, NotificationEvent> record, Acknowledgment ack) {
        NotificationEvent event = record.value();
        log.debug("[{}] Received | eventId={} partition={} offset={}",
                CHANNEL, event.getEventId(), record.partition(), record.offset());

        if (notificationService.isTerminal(event.getEventId())) {
            log.info("[{}] Skipping already completed eventId={}", CHANNEL, event.getEventId());
            ack.acknowledge();
            return;
        }

        // ── Rate limiting: acquire permit before calling provider ────────────
        // Blocks if we're over the configured rate (10/sec for email).
        // Mirrors real SendGrid/SES per-second API constraints.
        rateLimiter.acquire(CHANNEL);

        // ── Timed delivery attempt ───────────────────────────────────────────
        Timer.Sample timerSample = metrics.startDeliveryTimer();
        boolean success = false;
        try {
            deliveryProvider.deliver(CHANNEL, event.getRecipient(), event.getMessage(), event.getEventId());
            success = true;
            notificationService.markDelivered(event.getEventId(), CHANNEL);
            ack.acknowledge(); // commit offset only after successful DB write
            log.info("[{}] ✓ DELIVERED | eventId={}", CHANNEL, event.getEventId());
        } catch (DeliveryException e) {
            metrics.recordFailed(CHANNEL);
            log.error("[{}] ✗ FAILED | eventId={} | reason={}", CHANNEL, event.getEventId(), e.getMessage());
            throw e; // re-throw → DefaultErrorHandler retries → DLQ
        } finally {
            metrics.stopDeliveryTimer(timerSample, CHANNEL, success);
        }
    }
}
