package com.notificationservice.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The event model that travels over Kafka topics.
 *
 * Design note: this is a flat DTO — no JPA annotations — because it must be
 * JSON-serializable across producer and consumer boundaries. Keeping it
 * separate from the JPA entity avoids coupling the DB schema to the wire format.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationEvent {

    @JsonProperty("eventId")
    private String eventId;

    @JsonProperty("userId")
    private String userId;

    @JsonProperty("channel")
    private String channel;          // EMAIL | SMS | PUSH

    @JsonProperty("recipient")
    private String recipient;        // email address, phone number, or device token

    @JsonProperty("message")
    private String message;

    @JsonProperty("retryCount")
    @Builder.Default
    private int retryCount = 0;      // carried in the event for observability in DLQ

    @JsonProperty("originalTopic")
    private String originalTopic;    // populated by DLQ consumer for logging
}
