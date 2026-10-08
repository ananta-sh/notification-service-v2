package com.notificationservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.notificationservice.dto.NotificationRequest;
import com.notificationservice.dto.NotificationResponse;
import com.notificationservice.entity.Notification;
import com.notificationservice.entity.NotificationEvent;
import com.notificationservice.metrics.NotificationMetrics;
import com.notificationservice.outbox.OutboxEvent;
import com.notificationservice.outbox.OutboxRepository;
import com.notificationservice.repository.NotificationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
@Slf4j
public class NotificationService {

    private final NotificationRepository repository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final NotificationMetrics metrics;

    @Value("${kafka.topics.email}") private String emailTopic;
    @Value("${kafka.topics.sms}")   private String smsTopic;
    @Value("${kafka.topics.push}")  private String pushTopic;

    public NotificationService(NotificationRepository repository,
                                OutboxRepository outboxRepository,
                                ObjectMapper objectMapper,
                                NotificationMetrics metrics) {
        this.repository = repository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    @Transactional
    public NotificationResponse processNotification(NotificationRequest request) {
        // Layer 1: idempotency check
        Optional<Notification> existing = repository.findByEventId(request.getEventId());
        if (existing.isPresent()) {
            log.info("[Service] Duplicate event: eventId={}", request.getEventId());
            metrics.recordDuplicate(request.getChannel());
            return NotificationResponse.from(existing.get(), 200, "Event already received — no duplicate processing");
        }

        // Persist PENDING
        Notification notification = Notification.builder()
                .eventId(request.getEventId())
                .userId(request.getUserId())
                .channel(Notification.Channel.valueOf(request.getChannel().toUpperCase()))
                .recipient(request.getRecipient())
                .message(request.getMessage())
                .status(Notification.NotificationStatus.PENDING)
                .retryCount(0)
                .build();
        notification = repository.save(notification);
        log.info("[Service] Saved PENDING | eventId={} channel={}", request.getEventId(), request.getChannel());

        // Write OutboxEvent IN SAME TRANSACTION as notification save
        // Both commit or both rollback — crash between DB write and Kafka publish is now safe
        NotificationEvent event = NotificationEvent.builder()
                .eventId(request.getEventId())
                .userId(request.getUserId())
                .channel(request.getChannel().toUpperCase())
                .recipient(request.getRecipient())
                .message(request.getMessage())
                .build();

        String topic = topicForChannel(request.getChannel().toUpperCase());
        try {
            String payload = objectMapper.writeValueAsString(event);
            outboxRepository.save(OutboxEvent.builder()
                    .eventId(request.getEventId())
                    .topic(topic)
                    .messageKey(request.getUserId())
                    .payload(payload)
                    .published(false)
                    .build());
            log.info("[Service] OutboxEvent saved | eventId={} userId={} → topic={}",
                    request.getEventId(), request.getUserId(), topic);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Event serialization failed", e);
        }

        metrics.recordSubmitted(request.getChannel());
        return NotificationResponse.from(notification, 202, "Accepted — notification queued for delivery");
    }

    @Transactional
    public void markDelivered(String eventId, String channel) {
        int updated = repository.updateStatus(eventId, Notification.NotificationStatus.DELIVERED);
        if (updated > 0) {
            log.info("[Service] Status → DELIVERED | eventId={}", eventId);
            metrics.recordDelivered(channel);
        }
    }

    @Transactional
    public void markFailed(String eventId, String channel, int retryCount) {
        int updated = repository.updateStatusAndRetryCount(eventId, Notification.NotificationStatus.FAILED, retryCount);
        if (updated > 0) {
            log.info("[Service] Status → FAILED | eventId={} retryCount={}", eventId, retryCount);
            metrics.recordDlq(channel);
        }
    }

    @Transactional(readOnly = true)
    public Optional<NotificationResponse> getByEventId(String eventId) {
        return repository.findByEventId(eventId)
                .map(n -> NotificationResponse.from(n, 200, "Found"));
    }

    private String topicForChannel(String channel) {
        return switch (channel) {
            case "EMAIL" -> emailTopic;
            case "SMS"   -> smsTopic;
            case "PUSH"  -> pushTopic;
            default -> throw new IllegalArgumentException("Unknown channel: " + channel);
        };
    }
}
