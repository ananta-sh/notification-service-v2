package com.notificationservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.notificationservice.dto.NotificationRequest;
import com.notificationservice.dto.NotificationResponse;
import com.notificationservice.entity.Notification;
import com.notificationservice.entity.NotificationEvent;
import com.notificationservice.outbox.OutboxEvent;
import com.notificationservice.outbox.OutboxRepository;
import com.notificationservice.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Locale;

@Service
@RequiredArgsConstructor
public class NotificationSubmissionWriter {

    private final NotificationRepository notificationRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topics.email}") private String emailTopic;
    @Value("${kafka.topics.sms}") private String smsTopic;
    @Value("${kafka.topics.push}") private String pushTopic;

    @Transactional
    public NotificationResponse create(NotificationRequest request) {
        String channel = request.getChannel().toUpperCase(Locale.ROOT);
        Notification notification = Notification.builder()
                .eventId(request.getEventId())
                .userId(request.getUserId())
                .channel(Notification.Channel.valueOf(channel))
                .recipient(request.getRecipient())
                .message(request.getMessage())
                .status(Notification.NotificationStatus.PENDING)
                .retryCount(0)
                .build();

        // Flush within this transaction so a concurrent duplicate is reported
        // here; the façade can then return the winning request's saved state.
        notification = notificationRepository.saveAndFlush(notification);

        NotificationEvent event = NotificationEvent.builder()
                .eventId(request.getEventId())
                .userId(request.getUserId())
                .channel(channel)
                .recipient(request.getRecipient())
                .message(request.getMessage())
                .build();

        try {
            outboxRepository.save(OutboxEvent.builder()
                    .eventId(request.getEventId())
                    .topic(topicForChannel(channel))
                    .messageKey(request.getUserId())
                    .payload(objectMapper.writeValueAsString(event))
                    .published(false)
                    .build());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Notification event serialization failed", e);
        }

        return NotificationResponse.from(notification, 202,
                "Accepted — notification queued for delivery");
    }

    private String topicForChannel(String channel) {
        return switch (channel) {
            case "EMAIL" -> emailTopic;
            case "SMS" -> smsTopic;
            case "PUSH" -> pushTopic;
            default -> throw new IllegalArgumentException("Unknown channel: " + channel);
        };
    }
}
