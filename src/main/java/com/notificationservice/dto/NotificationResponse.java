package com.notificationservice.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.notificationservice.entity.Notification;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NotificationResponse {

    private String eventId;
    private String userId;
    private String channel;
    private String recipient;
    private String status;
    private int retryCount;
    private String message;          // human-readable outcome message
    private int httpStatus;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    /**
     * Converts a Notification entity to a response DTO.
     *
     * @param notification the persisted entity
     * @param httpStatus   202 for new, 200 for duplicate
     * @param message      human-readable message for the caller
     */
    public static NotificationResponse from(Notification notification, int httpStatus, String message) {
        return NotificationResponse.builder()
                .eventId(notification.getEventId())
                .userId(notification.getUserId())
                .channel(notification.getChannel().name())
                .recipient(notification.getRecipient())
                .status(notification.getStatus().name())
                .retryCount(notification.getRetryCount())
                .message(message)
                .httpStatus(httpStatus)
                .createdAt(notification.getCreatedAt())
                .updatedAt(notification.getUpdatedAt())
                .build();
    }
}
