package com.notificationservice.outbox;

/** Immutable snapshot handed to the Kafka publisher after the claim transaction commits. */
public record OutboxClaim(Long id, String eventId, String topic, String messageKey,
                          String payload, String claimToken) {
}
