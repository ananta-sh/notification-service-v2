package com.notificationservice.exception;

/**
 * Thrown by MockDeliveryService to simulate a failed notification delivery.
 *
 * Must be a RuntimeException so Spring Kafka's DefaultErrorHandler can catch
 * and apply retry + DLQ routing logic without forcing checked-exception handling
 * in every consumer method.
 */
public class DeliveryException extends RuntimeException {

    private final String eventId;
    private final String channel;

    public DeliveryException(String message, String eventId, String channel) {
        super(message);
        this.eventId = eventId;
        this.channel = channel;
    }

    public String getEventId() {
        return eventId;
    }

    public String getChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return String.format("DeliveryException{eventId='%s', channel='%s', message='%s'}",
                eventId, channel, getMessage());
    }
}
