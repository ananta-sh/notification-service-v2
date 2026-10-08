package com.notificationservice.service;

import com.notificationservice.exception.DeliveryException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Random;

/**
 * Simulates multi-channel notification delivery with a configurable failure rate.
 *
 * Each channel has realistic latency to reflect real-world differences:
 *   EMAIL: 500–1000ms  (SMTP servers, spam filters)
 *   SMS:   100–300ms   (carrier APIs are fast)
 *   PUSH:  50–150ms    (FCM/APNs are near-instant)
 *
 * A random 30% of calls throw DeliveryException, which triggers the Kafka
 * DefaultErrorHandler retry cycle → DLQ after max attempts.
 */
@Service
@Slf4j
public class MockDeliveryService {

    @Value("${mock.delivery.failure-rate:0.30}")
    private double failureRate;

    private final Random random = new Random();

    public void deliver(String channel, String recipient, String message, String eventId) {

        log.info("┌─ [{}] Attempting delivery | event={} | recipient={}",
                channel, eventId, maskRecipient(recipient, channel));

        // Simulate realistic network/API latency per channel
        simulateLatency(channel);

        // Randomly fail based on configured failure rate
        if (random.nextDouble() < failureRate) {
            log.warn("└─ [{}] ✗ DELIVERY FAILED (simulated) | event={}", channel, eventId);
            throw new DeliveryException(
                    String.format("Simulated delivery failure on channel=%s for event=%s", channel, eventId),
                    eventId,
                    channel
            );
        }

        log.info("└─ [{}] ✓ DELIVERY SUCCEEDED | event={} | recipient={}",
                channel, eventId, maskRecipient(recipient, channel));
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private void simulateLatency(String channel) {
        try {
            long latencyMs = switch (channel.toUpperCase()) {
                case "EMAIL" -> 500  + (long) (random.nextDouble() * 500);   // 500–1000ms
                case "SMS"   -> 100  + (long) (random.nextDouble() * 200);   // 100–300ms
                case "PUSH"  -> 50   + (long) (random.nextDouble() * 100);   // 50–150ms
                default      -> 200;
            };
            log.debug("  [{}] Simulating {}ms network latency", channel, latencyMs);
            Thread.sleep(latencyMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Mask PII for safe logging.
     * e.g., user@example.com → u***@example.com
     *       +1234567890      → +1****7890
     *       device-token-xyz → device-t***
     */
    private String maskRecipient(String recipient, String channel) {
        if (recipient == null || recipient.length() < 4) return "****";
        return switch (channel.toUpperCase()) {
            case "EMAIL" -> {
                int atIndex = recipient.indexOf('@');
                if (atIndex <= 1) yield "****";
                yield recipient.charAt(0) + "***" + recipient.substring(atIndex);
            }
            case "SMS" -> recipient.substring(0, 3) + "****" + recipient.substring(recipient.length() - 4);
            default    -> recipient.substring(0, 8) + "***";
        };
    }
}
