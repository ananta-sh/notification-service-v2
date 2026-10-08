package com.notificationservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Inbound REST request body for creating a notification.
 *
 * The client is responsible for generating a unique eventId (e.g., UUID v4).
 * This is the idempotency key — submitting the same eventId twice returns
 * the original result without duplicate processing.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationRequest {

    @NotBlank(message = "eventId is required — must be a client-generated UUID")
    private String eventId;

    @NotBlank(message = "userId is required")
    private String userId;

    @NotBlank(message = "channel is required")
    @Pattern(
        regexp = "(?i)EMAIL|SMS|PUSH",
        message = "channel must be one of: EMAIL, SMS, PUSH (case-insensitive)"
    )
    private String channel;

    @NotBlank(message = "recipient is required")
    private String recipient;

    @NotBlank(message = "message is required")
    private String message;
}
