package com.notificationservice.outbox;

import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    long countByEventId(String eventId);

    /**
     * Selects only the oldest unpublished row per topic/key. The pessimistic
     * lock and lease update are performed in one transaction so app replicas
     * cannot claim the same event. Keeping later rows blocked preserves order.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT o FROM OutboxEvent o
            WHERE o.published = false
              AND (o.claimUntil IS NULL OR o.claimUntil < :now)
              AND NOT EXISTS (
                  SELECT earlier.id FROM OutboxEvent earlier
                  WHERE earlier.topic = o.topic
                    AND earlier.messageKey = o.messageKey
                    AND earlier.published = false
                    AND earlier.id < o.id
              )
            ORDER BY o.createdAt ASC, o.id ASC
            """)
    List<OutboxEvent> findClaimable(@Param("now") LocalDateTime now, Pageable pageable);

    @Modifying
    @Query("""
            UPDATE OutboxEvent o
            SET o.published = true, o.publishedAt = :publishedAt,
                o.claimToken = null, o.claimUntil = null
            WHERE o.id = :id AND o.claimToken = :claimToken AND o.published = false
            """)
    int markAsPublished(@Param("id") Long id,
                        @Param("claimToken") String claimToken,
                        @Param("publishedAt") LocalDateTime publishedAt);

    @Modifying
    @Query("UPDATE OutboxEvent o SET o.claimUntil = :claimUntil " +
            "WHERE o.id = :id AND o.claimToken = :claimToken AND o.published = false")
    int renewClaim(@Param("id") Long id,
                   @Param("claimToken") String claimToken,
                   @Param("claimUntil") LocalDateTime claimUntil);

    @Modifying
    @Query("""
            UPDATE OutboxEvent o
            SET o.attemptCount = o.attemptCount + 1,
                o.claimToken = null, o.claimUntil = null
            WHERE o.id = :id AND o.claimToken = :claimToken AND o.published = false
            """)
    int releaseAfterFailure(@Param("id") Long id, @Param("claimToken") String claimToken);
}
