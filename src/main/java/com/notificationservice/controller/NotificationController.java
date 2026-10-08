package com.notificationservice.controller;

import com.notificationservice.dto.NotificationRequest;
import com.notificationservice.dto.NotificationResponse;
import com.notificationservice.service.NotificationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API for the notification service.
 *
 * POST /api/v1/notifications        → create notification (idempotent)
 * GET  /api/v1/notifications/{id}   → poll delivery status
 * GET  /api/v1/notifications/health → quick liveness check
 */
@RestController
@RequestMapping("/api/v1/notifications")
@Slf4j
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationService notificationService;

    /**
     * Submit a notification for delivery.
     *
     * Returns:
     *   202 Accepted  — new event queued
     *   200 OK        — duplicate eventId, returning existing state
     *   400 Bad Request — validation failure
     */
    @PostMapping
    public ResponseEntity<NotificationResponse> createNotification(
            @Valid @RequestBody NotificationRequest request) {

        log.info("POST /notifications | eventId={} channel={}", request.getEventId(), request.getChannel());

        NotificationResponse response = notificationService.processNotification(request);

        return ResponseEntity
                .status(response.getHttpStatus())
                .body(response);
    }

    /**
     * Poll the current status of a notification by its eventId.
     *
     * Returns:
     *   200 OK    — found, body contains current status (PENDING / DELIVERED / FAILED)
     *   404       — no notification with this eventId
     */
    @GetMapping("/{eventId}")
    public ResponseEntity<NotificationResponse> getNotification(@PathVariable String eventId) {

        log.debug("GET /notifications/{}", eventId);

        return notificationService.getByEventId(eventId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Simple health check endpoint.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "notification-service"
        ));
    }

    /**
     * Global validation error handler — returns structured 400 with field errors.
     */
    @ExceptionHandler(org.springframework.web.bind.MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidationErrors(
            org.springframework.web.bind.MethodArgumentNotValidException ex) {

        Map<String, String> fieldErrors = new java.util.LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(e -> fieldErrors.put(e.getField(), e.getDefaultMessage()));

        return ResponseEntity.badRequest().body(Map.of(
                "status", 400,
                "error", "Validation Failed",
                "fields", fieldErrors
        ));
    }
}
