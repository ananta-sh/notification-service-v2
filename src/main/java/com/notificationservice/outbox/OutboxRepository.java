package com.notificationservice.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * Core query used by the OutboxPoller every 2 seconds.
     * Fetches at most 100 rows to avoid overwhelming Kafka in one batch.
     * Ordered by created_at to preserve original submission order.
     */
    List<OutboxEvent> findByPublishedFalseOrderByCreatedAtAsc(Pageable pageable);

    /**
     * Mark a single event as successfully published.
     * Called by OutboxPoller after Kafka confirms receipt.
     */
    @Modifying
    @Transactional
    @Query("UPDATE OutboxEvent o SET o.published = true, o.publishedAt = :publishedAt WHERE o.id = :id")
    int markAsPublished(@Param("id") Long id, @Param("publishedAt") LocalDateTime publishedAt);

    /**
     * Increment attempt counter — used for observability and
     * identifying stuck events (attemptCount > 10 = needs manual review).
     */
    @Modifying
    @Transactional
    @Query("UPDATE OutboxEvent o SET o.attemptCount = o.attemptCount + 1 WHERE o.id = :id")
    int incrementAttemptCount(@Param("id") Long id);
}
