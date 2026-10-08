package com.notificationservice.outbox;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * ═══════════════════════════════════════════════════════════════════
 * OUTBOX PATTERN — Why this exists
 * ═══════════════════════════════════════════════════════════════════
 *
 * Problem: In NotificationService.processNotification(), we do two things:
 *   1. Save Notification to DB  (transactional)
 *   2. Publish event to Kafka   (NOT transactional)
 *
 * If the JVM crashes between step 1 and step 2, the notification is
 * in DB with status PENDING forever — Kafka never gets the message.
 * The event is silently lost.
 *
 * Solution (Transactional Outbox Pattern):
 *   1. Save Notification to DB           ─┐
 *   2. Save OutboxEvent to DB             ├─ ONE atomic transaction
 *      (same transaction, both or neither)┘
 *   3. OutboxPoller (separate thread) reads unpublished outbox events
 *   4. Publishes each to Kafka
 *   5. Marks outbox event as PUBLISHED
 *
 * Now a crash between steps 2→3 is safe: on restart, the poller
 * finds the unpublished outbox event and retries.
 *
 * This is the standard solution used by Debezium, Eventuate, and
 * every serious event-driven system.
 * ═══════════════════════════════════════════════════════════════════
 */
@Entity
@Table(name = "outbox_events",
    indexes = {
        @Index(name = "idx_outbox_published", columnList = "published"),
        @Index(name = "idx_outbox_created_at", columnList = "created_at"),
        @Index(name = "idx_outbox_claim", columnList = "published, claim_until")
    }
)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * The eventId from the original notification request.
     * Links this outbox row back to the notifications table.
     */
    @Column(name = "event_id", nullable = false, length = 255)
    private String eventId;

    /**
     * Kafka topic this event should be published to.
     * e.g., "notification.email"
     */
    @Column(name = "topic", nullable = false, length = 100)
    private String topic;

    /**
     * Kafka message key (= userId for per-user partition ordering).
     */
    @Column(name = "message_key", nullable = false, length = 255)
    private String messageKey;

    /**
     * Full JSON payload of the NotificationEvent.
     * Stored as text so no Kafka dependency in the DB layer.
     */
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    /**
     * FALSE until the OutboxPoller successfully publishes to Kafka.
     * The poller queries WHERE published = FALSE.
     */
    @Column(name = "published", nullable = false)
    @Builder.Default
    private boolean published = false;

    /**
     * How many publish attempts have been made.
     * If this exceeds a threshold, the row can be flagged for manual review.
     */
    @Column(name = "attempt_count")
    @Builder.Default
    private int attemptCount = 0;

    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "published_at")
    private LocalDateTime publishedAt;

    @Column(name = "claim_token", length = 36)
    private String claimToken;

    @Column(name = "claim_until")
    private LocalDateTime claimUntil;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }
}
