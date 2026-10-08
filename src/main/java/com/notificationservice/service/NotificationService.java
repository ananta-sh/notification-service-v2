package com.notificationservice.service;

import com.notificationservice.dto.NotificationRequest;
import com.notificationservice.dto.NotificationResponse;
import com.notificationservice.entity.Notification;
import com.notificationservice.metrics.NotificationMetrics;
import com.notificationservice.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {

    private final NotificationRepository repository;
    private final NotificationSubmissionWriter submissionWriter;
    private final NotificationMetrics metrics;

    public NotificationResponse processNotification(NotificationRequest request) {
        Optional<Notification> existing = repository.findByEventId(request.getEventId());
        if (existing.isPresent()) {
            return duplicate(existing.get(), request.getChannel());
        }

        try {
            NotificationResponse response = submissionWriter.create(request);
            metrics.recordSubmitted(request.getChannel());
            return response;
        } catch (DataIntegrityViolationException concurrentDuplicate) {
            // The unique event_id constraint resolves the race between two
            // simultaneous first submissions. The writer transaction has
            // rolled back before this lookup executes.
            return repository.findByEventId(request.getEventId())
                    .map(notification -> duplicate(notification, request.getChannel()))
                    .orElseThrow(() -> concurrentDuplicate);
        }
    }

    @Transactional
    public void markDelivered(String eventId, String channel) {
        int updated = repository.updateStatusIfCurrent(eventId,
                Notification.NotificationStatus.PENDING,
                Notification.NotificationStatus.DELIVERED);
        if (updated > 0) {
            log.info("[Service] Status → DELIVERED | eventId={}", eventId);
            metrics.recordDelivered(channel);
        }
    }

    @Transactional
    public void markFailed(String eventId, String channel, int retryCount) {
        int updated = repository.updateStatusAndRetryCountIfCurrent(eventId,
                Notification.NotificationStatus.PENDING,
                Notification.NotificationStatus.FAILED,
                retryCount);
        if (updated > 0) {
            log.info("[Service] Status → FAILED | eventId={} retryCount={}", eventId, retryCount);
            metrics.recordDlq(channel);
        }
    }

    @Transactional(readOnly = true)
    public boolean isTerminal(String eventId) {
        return repository.findByEventId(eventId)
                .map(n -> n.getStatus() != Notification.NotificationStatus.PENDING)
                .orElse(false);
    }

    @Transactional(readOnly = true)
    public Optional<NotificationResponse> getByEventId(String eventId) {
        return repository.findByEventId(eventId)
                .map(n -> NotificationResponse.from(n, 200, "Found"));
    }

    private NotificationResponse duplicate(Notification notification, String channel) {
        log.info("[Service] Duplicate event: eventId={}", notification.getEventId());
        metrics.recordDuplicate(channel);
        return NotificationResponse.from(notification, 200,
                "Event already received — no duplicate processing");
    }
}
