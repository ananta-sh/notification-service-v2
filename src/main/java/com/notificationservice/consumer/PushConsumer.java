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
public class PushConsumer {

    private final NotificationService notificationService;
    private final NotificationDeliveryProvider deliveryProvider;
    private final ChannelRateLimiterService rateLimiter;
    private final NotificationMetrics metrics;

    private static final String CHANNEL = "PUSH";

    @KafkaListener(
        topics = "${kafka.topics.push}",
        groupId = "notification-push-group",
        concurrency = "3",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, NotificationEvent> record, Acknowledgment ack) {
        NotificationEvent event = record.value();
        log.debug("[{}] Received | eventId={} partition={}", CHANNEL, event.getEventId(), record.partition());

        if (notificationService.isTerminal(event.getEventId())) {
            log.info("[{}] Skipping already completed eventId={}", CHANNEL, event.getEventId());
            ack.acknowledge();
            return;
        }

        rateLimiter.acquire(CHANNEL); // 100/sec — FCM/APNs constraint

        Timer.Sample timerSample = metrics.startDeliveryTimer();
        boolean success = false;
        try {
            deliveryProvider.deliver(CHANNEL, event.getRecipient(), event.getMessage(), event.getEventId());
            success = true;
            notificationService.markDelivered(event.getEventId(), CHANNEL);
            ack.acknowledge();
            log.info("[{}] ✓ DELIVERED | eventId={}", CHANNEL, event.getEventId());
        } catch (DeliveryException e) {
            metrics.recordFailed(CHANNEL);
            log.error("[{}] ✗ FAILED | eventId={} | reason={}", CHANNEL, event.getEventId(), e.getMessage());
            throw e;
        } finally {
            metrics.stopDeliveryTimer(timerSample, CHANNEL, success);
        }
    }
}
