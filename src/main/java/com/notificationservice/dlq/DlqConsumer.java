package com.notificationservice.dlq;

import com.notificationservice.entity.NotificationEvent;
import com.notificationservice.metrics.NotificationMetrics;
import com.notificationservice.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class DlqConsumer {

    private final NotificationService notificationService;
    private final NotificationMetrics metrics;

    @KafkaListener(
        topics = "${kafka.topics.dlq}",
        groupId = "notification-dlq-group",
        containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, NotificationEvent> record, Acknowledgment ack) {
        NotificationEvent event = record.value();
        log.error("╔══════════════════════════════════════════════════════");
        log.error("║  DLQ — PERMANENT DELIVERY FAILURE");
        log.error("║  eventId  : {}", event.getEventId());
        log.error("║  channel  : {}", event.getChannel());
        log.error("║  recipient: {}", maskRecipient(event.getRecipient(), event.getChannel()));
        log.error("╚══════════════════════════════════════════════════════");

        // Pass channel from the event for accurate per-channel DLQ metrics
        notificationService.markFailed(event.getEventId(), event.getChannel(), 3);
        ack.acknowledge();
    }

    private String maskRecipient(String recipient, String channel) {
        if (recipient == null || recipient.length() < 4) return "***";
        return switch (channel.toUpperCase()) {
            case "EMAIL" -> recipient.replaceAll("(.).+(@.+)", "$1***$2");
            case "SMS"   -> recipient.substring(0, recipient.length() - 4) + "****";
            default      -> recipient.substring(0, 6) + "***";
        };
    }
}
