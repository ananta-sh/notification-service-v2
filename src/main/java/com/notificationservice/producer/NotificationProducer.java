package com.notificationservice.producer;

import com.notificationservice.entity.NotificationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes NotificationEvents to channel-specific Kafka topics.
 *
 * Routing: channel → topic
 *   EMAIL → notification.email
 *   SMS   → notification.sms
 *   PUSH  → notification.push
 *
 * The userId is used as the Kafka message key, so events for a user are
 * assigned to the same partition and retain their order within each topic.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class NotificationProducer {

    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;

    @Value("${kafka.topics.email}")
    private String emailTopic;

    @Value("${kafka.topics.sms}")
    private String smsTopic;

    @Value("${kafka.topics.push}")
    private String pushTopic;

    public void publish(NotificationEvent event) {
        String topic = resolveTopic(event.getChannel());

        log.info("Publishing event={} to topic={}", event.getEventId(), topic);

        CompletableFuture<SendResult<String, NotificationEvent>> future =
                kafkaTemplate.send(topic, event.getUserId(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                // Producer-level failure: Kafka unavailable or serialization error.
                // The REST layer has already returned 202, so we log here.
                // Production systems would trigger an alert or fallback queue.
                log.error("PRODUCER ERROR: Failed to publish event={} to topic={} | error={}",
                        event.getEventId(), topic, ex.getMessage());
            } else {
                log.debug("Event={} user={} published → topic={} partition={} offset={}",
                        event.getEventId(),
                        event.getUserId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    // ── Private ──────────────────────────────────────────────────────────────

    private String resolveTopic(String channel) {
        return switch (channel.toUpperCase()) {
            case "EMAIL" -> emailTopic;
            case "SMS"   -> smsTopic;
            case "PUSH"  -> pushTopic;
            default -> throw new IllegalArgumentException(
                    "Unknown channel: " + channel + ". Valid values: EMAIL, SMS, PUSH"
            );
        };
    }
}
