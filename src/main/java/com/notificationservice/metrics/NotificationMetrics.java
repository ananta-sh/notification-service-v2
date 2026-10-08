package com.notificationservice.metrics;

import io.micrometer.core.instrument.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Centralized Micrometer metrics for the notification service.
 *
 * All metrics are accessible via:
 *   GET /actuator/metrics                         → list all metric names
 *   GET /actuator/metrics/notifications.submitted → specific metric
 *
 * Metrics exposed:
 * ┌─────────────────────────────────────────────────────────────────┐
 * │ Counter: notifications.submitted      (channel tag)             │
 * │ Counter: notifications.delivered      (channel tag)             │
 * │ Counter: notifications.failed         (channel tag)             │
 * │ Counter: notifications.dlq            (channel tag)             │
 * │ Counter: notifications.duplicate      (channel tag)             │
 * │ Timer:   notifications.delivery.time  (channel tag)             │
 * │ Gauge:   notifications.pending.count  (live DB count)           │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * All counters have a `channel` tag (EMAIL, SMS, PUSH) so you can
 * slice metrics per channel in Grafana/Prometheus:
 *   notifications.delivered{channel="EMAIL"}
 *   notifications.delivered{channel="SMS"}
 */
@Component
@Slf4j
public class NotificationMetrics {

    private final MeterRegistry registry;

    // Live pending count per channel — backed by AtomicLong for thread safety
    private final ConcurrentHashMap<String, AtomicLong> pendingCounts = new ConcurrentHashMap<>();

    public NotificationMetrics(MeterRegistry registry) {
        this.registry = registry;

        // Register gauges for each channel — live values
        for (String channel : new String[]{"EMAIL", "SMS", "PUSH"}) {
            AtomicLong count = new AtomicLong(0);
            pendingCounts.put(channel, count);
            Gauge.builder("notifications.pending.count", count, AtomicLong::get)
                    .tag("channel", channel)
                    .description("Number of notifications currently in PENDING state")
                    .register(registry);
        }
    }

    // ── Submission ────────────────────────────────────────────────────────

    public void recordSubmitted(String channel) {
        Counter.builder("notifications.submitted.total")
                .tag("channel", channel.toUpperCase())
                .description("Total notifications submitted via REST API")
                .register(registry)
                .increment();
        pendingCounts.computeIfAbsent(channel.toUpperCase(), k -> new AtomicLong(0))
                .incrementAndGet();
    }

    public void recordDuplicate(String channel) {
        Counter.builder("notifications.duplicate.total")
                .tag("channel", channel.toUpperCase())
                .description("Duplicate eventId submissions rejected (idempotency)")
                .register(registry)
                .increment();
    }

    // ── Delivery outcomes ─────────────────────────────────────────────────

    public void recordDelivered(String channel) {
        Counter.builder("notifications.delivered.total")
                .tag("channel", channel.toUpperCase())
                .description("Notifications successfully delivered")
                .register(registry)
                .increment();
        pendingCounts.computeIfAbsent(channel.toUpperCase(), k -> new AtomicLong(0))
                .decrementAndGet();
    }

    public void recordFailed(String channel) {
        Counter.builder("notifications.failed.total")
                .tag("channel", channel.toUpperCase())
                .description("Notification delivery attempts that failed (will retry)")
                .register(registry)
                .increment();
    }

    public void recordDlq(String channel) {
        Counter.builder("notifications.dlq.total")
                .tag("channel", channel.toUpperCase())
                .description("Notifications permanently failed and moved to DLQ")
                .register(registry)
                .increment();
        pendingCounts.computeIfAbsent(channel.toUpperCase(), k -> new AtomicLong(0))
                .decrementAndGet();
    }

    // ── Delivery timing ───────────────────────────────────────────────────

    /**
     * Records how long a delivery attempt took.
     * Use with try-with-resources:
     *
     *   try (Timer.Sample sample = metrics.startDeliveryTimer()) {
     *       mockDeliveryService.deliver(...);
     *   }
     */
    public Timer.Sample startDeliveryTimer() {
        return Timer.start(registry);
    }

    public void stopDeliveryTimer(Timer.Sample sample, String channel, boolean success) {
        sample.stop(Timer.builder("notifications.delivery.duration")
                .tag("channel", channel.toUpperCase())
                .tag("success", String.valueOf(success))
                .description("Time taken for a delivery attempt (including simulated latency)")
                .register(registry));
    }

    // ── Rate limiter metrics ──────────────────────────────────────────────

    public void recordRateLimitWait(String channel, long waitMs) {
        if (waitMs > 100) { // only log meaningful waits
            log.debug("[RateLimit] {} consumer waited {}ms for rate limiter permit", channel, waitMs);
        }
        Counter.builder("notifications.ratelimit.waits.total")
                .tag("channel", channel.toUpperCase())
                .description("Number of times a consumer waited for a rate limit permit")
                .register(registry)
                .increment();
    }
}
