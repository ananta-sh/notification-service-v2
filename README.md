# Distributed Notification Service

A distributed notification service built with **Spring Boot**, **Apache Kafka**, and **MySQL**. It routes email, SMS, and push notifications to isolated Kafka topics and consumer groups, with a transactional outbox, idempotent submission, retries, and dead-letter handling. Provider delivery is simulated by `MockDeliveryService`.

---

## Architecture

```
Client
  │
  ▼
POST /api/v1/notifications
  │
  ├─ Idempotency Check (DB unique constraint on eventId)
  │    ├─ Duplicate → 200 OK  (no reprocessing)
  │    └─ New       → Save PENDING + outbox row → 202 Accepted
  │                                    │
  │                             Outbox poller publishes
  │
  ▼
Kafka Topics (3 partitions each; Kafka key = userId)
  ├─ notification.email ──► EmailConsumer ─┐
  ├─ notification.sms   ──► SmsConsumer   ─┤─► MockDeliveryService (30% failure)
  └─ notification.push  ──► PushConsumer  ─┘
                                           │
                              Success: DB → DELIVERED + ack offset
                              Failure: DefaultErrorHandler retries (3 attempts)
                                           │
                                      notification.dlq
                                           │
                                      DlqConsumer
                                           │
                                      DB → FAILED + ack offset
```

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Framework | Spring Boot 3.5.16 |
| Messaging | Apache Kafka (embedded broker for local runs) |
| Database  | H2 by default; MySQL optional (via Spring Data JPA) |
| Retry/DLQ | Spring Kafka DefaultErrorHandler + DeadLetterPublishingRecoverer |
| Container | Docker + Docker Compose |
| Build     | Maven |
| Java      | 17 |

---

## Quick Start

### Run locally without Docker

```bash
mvn spring-boot:run
```

The app starts with an embedded Kafka broker and an in-memory H2 database. Kafka topics are created automatically. No Kafka container or Docker installation is needed.

To run with MySQL instead, start the optional database container:

```bash
docker compose up -d mysql
```

Then set `SPRING_DATASOURCE_URL`, `SPRING_DATASOURCE_USERNAME`, and `SPRING_DATASOURCE_PASSWORD` to the MySQL connection details before launching the app. The app starts on **port 8080**. The embedded broker listens on port **19092**; override `SPRING_KAFKA_BOOTSTRAP_SERVERS` only when using an external Kafka broker.

---

## API Usage

### Submit a notification (EMAIL)

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "550e8400-e29b-41d4-a716-446655440000",
    "userId": "user-123",
    "channel": "EMAIL",
    "recipient": "user@example.com",
    "message": "Your order #12345 has been confirmed!"
  }'
```

Response `202 Accepted`:
```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "user-123",
  "channel": "EMAIL",
  "recipient": "user@example.com",
  "status": "PENDING",
  "retryCount": 0,
  "message": "Accepted — notification queued for delivery",
  "httpStatus": 202
}
```

### Submit same eventId again (idempotency test)

```bash
# Same curl command — returns 200, not 202
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "550e8400-e29b-41d4-a716-446655440000",
    "userId": "user-123",
    "channel": "EMAIL",
    "recipient": "user@example.com",
    "message": "Your order #12345 has been confirmed!"
  }'
```

Response `200 OK`:
```json
{
  "status": "DELIVERED",
  "message": "Event already received — no duplicate processing",
  "httpStatus": 200
}
```

### Check notification status

```bash
curl http://localhost:8080/api/v1/notifications/550e8400-e29b-41d4-a716-446655440000
```

### Try SMS channel

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "660e8400-e29b-41d4-a716-446655440001",
    "userId": "user-123",
    "channel": "SMS",
    "recipient": "+14155552671",
    "message": "Your verification code is 847291"
  }'
```

### Try PUSH channel

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "770e8400-e29b-41d4-a716-446655440002",
    "userId": "user-456",
    "channel": "PUSH",
    "recipient": "device-token-abc123xyz",
    "message": "You have a new message from Alice"
  }'
```

---

## Key Design Decisions

### Why manual Kafka offset commit?
Auto-commit risks losing messages if the consumer crashes between reading a record and writing to the DB. Manual commit (via `Acknowledgment.acknowledge()`) means the offset is only advanced _after_ the DB write succeeds — guaranteeing at-least-once delivery.

### Why two layers of idempotency?
- **Application layer:** `findByEventId()` before every insert — fast path that avoids unnecessary DB writes.
- **Database layer:** `UNIQUE` constraint on `event_id` — last-resort guard if two concurrent requests slip past the application check simultaneously.

### Why separate topics per channel?
A slow email SMTP server shouldn't block SMS or push delivery. Independent topics mean independent consumer groups and independent scaling — add more email consumers without touching SMS infrastructure.

### How is per-user ordering maintained?
The producer and transactional outbox use `userId` as the Kafka record key. Kafka assigns records with the same key to the same partition, preserving their order within each channel topic while allowing different partitions to be processed in parallel. Separate channel topics do not provide a total order across email, SMS, and push.

### Why `DefaultErrorHandler` over `@RetryableTopic`?
`DefaultErrorHandler` with `FixedBackOff` retries in-memory (no additional Kafka topics created, no extra consumer group lag). For local dev and portfolio purposes this is simpler to reason about. `@RetryableTopic` is preferable in production when you need durable retry queues (retries survive app restarts).

### Why 3 partitions?
Each consumer group can have at most one active consumer per partition. Three partitions allow up to three active consumers per channel group; scale each channel independently.

---

## Project Structure

```
src/main/java/com/notificationservice/
├── NotificationServiceApplication.java
├── config/
│   └── KafkaConfig.java          # Topics, producer/consumer factories, error handler
├── controller/
│   └── NotificationController.java
├── dto/
│   ├── NotificationRequest.java
│   └── NotificationResponse.java
├── entity/
│   ├── Notification.java          # JPA entity (MySQL)
│   └── NotificationEvent.java     # Kafka wire model
├── repository/
│   └── NotificationRepository.java
├── producer/
│   └── NotificationProducer.java
├── consumer/
│   ├── EmailConsumer.java
│   ├── SmsConsumer.java
│   └── PushConsumer.java
├── dlq/
│   └── DlqConsumer.java
├── service/
│   ├── NotificationService.java   # Business logic + idempotency
│   └── MockDeliveryService.java   # Simulated delivery (30% failure rate)
└── exception/
    └── DeliveryException.java
```

---

## Observing DLQ Behavior

Because the mock service fails ~30% of the time, some events will exhaust retries and land in the DLQ. Watch the logs:

```
WARN  Retry attempt 1/3 | topic=notification.email | key=<userId>
WARN  Retry attempt 2/3 | topic=notification.email | key=<userId>
ERROR Moving event to DLQ after 2 retries | topic=notification.email
ERROR ╔══════════════════════════════════════════════
ERROR ║ DLQ MESSAGE RECEIVED — PERMANENT DELIVERY FAILURE
ERROR ║ eventId: <eventId>
...
```

Then poll the status endpoint to confirm the event is marked `FAILED`.

---

## Teardown

```bash
# Stop the app (Ctrl+C), then:
docker compose down -v   # -v removes the optional MySQL volume (clean slate)
```
