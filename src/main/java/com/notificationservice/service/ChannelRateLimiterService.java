package com.notificationservice.service;

import com.google.common.util.concurrent.RateLimiter;
import com.notificationservice.metrics.NotificationMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Per-channel rate limiting using Guava's RateLimiter (token bucket algorithm).
 *
 * Why rate limiting matters in a real notification service:
 * ──────────────────────────────────────────────────────────
 * - SendGrid (email):  ~100 emails/sec on free tier, ~1000/sec paid
 * - Twilio (SMS):      ~1 msg/sec per number, ~1000/sec with pools
 * - FCM/APNs (push):  ~500/sec per connection
 *
 * Without rate limiting, a burst of 10,000 notifications would cause:
 *   1. Provider API returns HTTP 429 (Too Many Requests)
 *   2. Kafka consumer retries → fills retry queue
 *   3. Cascades into DLQ storm
 *
 * With rate limiting, the consumer self-throttles BEFORE calling the
 * provider, absorbing the burst gracefully.
 *
 * RateLimiter.acquire() blocks the consumer thread until a permit is
 * available — this is intentional. The Kafka consumer is already on its
 * own thread, so blocking here doesn't starve other operations.
 */
@Service
@Slf4j
public class ChannelRateLimiterService {

    private final Map<String, RateLimiter> rateLimiters;
    private final NotificationMetrics metrics;

    public ChannelRateLimiterService(
            @Value("${rate-limit.email:10.0}") double emailRate,
            @Value("${rate-limit.sms:50.0}")   double smsRate,
            @Value("${rate-limit.push:100.0}") double pushRate,
            NotificationMetrics metrics) {

        this.metrics = metrics;
        this.rateLimiters = Map.of(
                "EMAIL", RateLimiter.create(emailRate),
                "SMS",   RateLimiter.create(smsRate),
                "PUSH",  RateLimiter.create(pushRate)
        );

        log.info("╔══════════════════════════════════════════════════════");
        log.info("║  RATE LIMITERS INITIALIZED");
        log.info("║  EMAIL : {} permits/sec  (SMTP provider limit)", emailRate);
        log.info("║  SMS   : {} permits/sec  (Carrier API limit)",   smsRate);
        log.info("║  PUSH  : {} permits/sec  (FCM/APNs limit)",      pushRate);
        log.info("╚══════════════════════════════════════════════════════");
    }

    /**
     * Acquires a rate limit permit for the given channel.
     * Blocks until a permit is available (token bucket refill).
     *
     * @param channel EMAIL, SMS, or PUSH
     * @return time waited in milliseconds (for metrics/logging)
     */
    public double acquire(String channel) {
        RateLimiter limiter = rateLimiters.get(channel.toUpperCase());
        if (limiter == null) {
            log.warn("No rate limiter found for channel: {} — skipping", channel);
            return 0.0;
        }

        long startMs = System.currentTimeMillis();
        double waitTime = limiter.acquire(); // blocks here if rate exceeded
        long waitMs = System.currentTimeMillis() - startMs;

        if (waitMs > 50) {
            // Only log noticeable waits — sub-50ms is normal scheduling jitter
            log.debug("[RateLimit] {} waited {}ms for permit (rate: {}/sec)",
                    channel, waitMs, getRateForChannel(channel));
            metrics.recordRateLimitWait(channel, waitMs);
        }

        return waitTime;
    }

    private double getRateForChannel(String channel) {
        RateLimiter limiter = rateLimiters.get(channel.toUpperCase());
        return limiter != null ? limiter.getRate() : 0;
    }
}
