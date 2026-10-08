package com.notificationservice.service;

/**
 * Adapter boundary for channel delivery. Implementations should pass the
 * idempotency key to providers that support request deduplication.
 */
public interface NotificationDeliveryProvider {

    void deliver(String channel, String recipient, String message, String idempotencyKey);
}
