package com.notificationservice.repository;

import com.notificationservice.entity.Notification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface NotificationRepository extends JpaRepository<Notification, Long> {

    /**
     * Primary idempotency lookup — called before every new notification
     * to check whether this eventId has already been processed.
     */
    Optional<Notification> findByEventId(String eventId);

    /**
     * Efficient status update without loading the full entity.
     * Used by consumers after successful delivery.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Notification n SET n.status = :status, n.updatedAt = CURRENT_TIMESTAMP " +
            "WHERE n.eventId = :eventId AND n.status = :expectedStatus")
    int updateStatusIfCurrent(@Param("eventId") String eventId,
                              @Param("expectedStatus") Notification.NotificationStatus expectedStatus,
                              @Param("status") Notification.NotificationStatus status);

    /**
     * Update status AND retry count atomically.
     * Used by DLQ consumer when a notification is permanently failed.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Notification n SET n.status = :status, n.retryCount = :retryCount, " +
            "n.updatedAt = CURRENT_TIMESTAMP WHERE n.eventId = :eventId " +
            "AND n.status = :expectedStatus")
    int updateStatusAndRetryCountIfCurrent(
        @Param("eventId") String eventId,
        @Param("expectedStatus") Notification.NotificationStatus expectedStatus,
        @Param("status") Notification.NotificationStatus status,
        @Param("retryCount") int retryCount
    );
}
