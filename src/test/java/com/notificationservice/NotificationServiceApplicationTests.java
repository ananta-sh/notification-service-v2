package com.notificationservice;

import com.notificationservice.dto.NotificationRequest;
import com.notificationservice.dto.NotificationResponse;
import com.notificationservice.consumer.EmailConsumer;
import com.notificationservice.entity.NotificationEvent;
import com.notificationservice.config.KafkaConfig;
import com.notificationservice.outbox.OutboxClaim;
import com.notificationservice.outbox.OutboxClaimService;
import com.notificationservice.outbox.OutboxEvent;
import com.notificationservice.outbox.OutboxRepository;
import com.notificationservice.metrics.NotificationMetrics;
import com.notificationservice.repository.NotificationRepository;
import com.notificationservice.service.NotificationService;
import com.notificationservice.service.NotificationSubmissionWriter;
import com.notificationservice.service.NotificationDeliveryProvider;
import com.notificationservice.service.ChannelRateLimiterService;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

import java.util.concurrent.atomic.AtomicInteger;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest(showSql = false)
@ActiveProfiles("test")
@Import({NotificationService.class, NotificationSubmissionWriter.class,
        OutboxClaimService.class, NotificationMetrics.class, ChannelRateLimiterService.class,
        EmailConsumer.class, KafkaConfig.class, NotificationServiceApplicationTests.TestBeans.class})
class NotificationServiceApplicationTests {

    @Autowired
    private NotificationService notificationService;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private OutboxClaimService outboxClaimService;

    @Autowired
    private NotificationRepository notificationRepository;

    @Autowired
    private EmailConsumer emailConsumer;

    @Autowired
    private TestBeans.TestDeliveryProvider testDeliveryProvider;

    @Test
    void contextLoads() {
        // Verifies the Spring context starts without errors
    }

    @Test
    void submissionIsIdempotentAndCreatesOnlyOneOutboxEvent() {
        String eventId = UUID.randomUUID().toString();
        NotificationRequest request = NotificationRequest.builder()
                .eventId(eventId)
                .userId("test-user")
                .channel("EMAIL")
                .recipient("test@example.com")
                .message("Test notification")
                .build();

        NotificationResponse first = notificationService.processNotification(request);
        NotificationResponse duplicate = notificationService.processNotification(request);

        assertThat(first.getHttpStatus()).isEqualTo(202);
        assertThat(duplicate.getHttpStatus()).isEqualTo(200);
        assertThat(outboxRepository.countByEventId(eventId)).isEqualTo(1);
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void concurrentSubmissionsForSameEventReturnOneAcceptedAndOneDuplicate() throws Exception {
        String eventId = UUID.randomUUID().toString();
        NotificationRequest request = NotificationRequest.builder()
                .eventId(eventId)
                .userId("concurrent-user")
                .channel("SMS")
                .recipient("+14155552671")
                .message("Concurrent test")
                .build();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> submitAfter(start, ready, request));
            var second = executor.submit(() -> submitAfter(start, ready, request));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            List<Integer> statuses = List.of(first.get(15, TimeUnit.SECONDS).getHttpStatus(),
                    second.get(15, TimeUnit.SECONDS).getHttpStatus());
            assertThat(statuses).containsExactlyInAnyOrder(202, 200);
        } finally {
            executor.shutdownNow();
        }

        assertThat(outboxRepository.countByEventId(eventId)).isEqualTo(1);
    }

    @Test
    void outboxClaimsDoNotOverlapAndKeepSameUserTopicOrder() {
        outboxRepository.deleteAllInBatch();
        String topic = "notification.email";
        outboxRepository.saveAndFlush(outbox("first-a", topic, "user-a"));
        outboxRepository.saveAndFlush(outbox("second-a", topic, "user-a"));
        outboxRepository.saveAndFlush(outbox("first-b", topic, "user-b"));

        List<OutboxClaim> firstBatch = outboxClaimService.claimBatch();
        List<OutboxClaim> secondBatch = outboxClaimService.claimBatch();

        assertThat(firstBatch).extracting(OutboxClaim::eventId)
                .containsExactlyInAnyOrder("first-a", "first-b");
        assertThat(secondBatch).isEmpty();

        OutboxClaim firstA = firstBatch.stream()
                .filter(claim -> claim.eventId().equals("first-a"))
                .findFirst().orElseThrow();
        assertThat(outboxClaimService.markPublished(firstA)).isTrue();

        assertThat(outboxClaimService.claimBatch()).extracting(OutboxClaim::eventId)
                .containsExactly("second-a");
    }

    @Test
    void terminalDeliveryStateCannotBeOverwrittenOrCountedTwice() {
        String eventId = UUID.randomUUID().toString();
        NotificationRequest request = NotificationRequest.builder()
                .eventId(eventId)
                .userId("delivery-user")
                .channel("EMAIL")
                .recipient("delivery@example.com")
                .message("Delivery status test")
                .build();
        notificationService.processNotification(request);

        notificationService.markDelivered(eventId, "EMAIL");
        notificationService.markDelivered(eventId, "EMAIL");
        notificationService.markFailed(eventId, "EMAIL", 3);

        assertThat(notificationService.isTerminal(eventId)).isTrue();
        assertThat(notificationRepository.findByEventId(eventId).orElseThrow().getStatus().name())
                .isEqualTo("DELIVERED");
    }

    @Test
    void replayAfterDeliveryIsAcknowledgedWithoutCallingProviderTwice() {
        String eventId = UUID.randomUUID().toString();
        NotificationRequest request = NotificationRequest.builder()
                .eventId(eventId)
                .userId("replay-user")
                .channel("EMAIL")
                .recipient("replay@example.com")
                .message("Replay test")
                .build();
        notificationService.processNotification(request);

        NotificationEvent event = NotificationEvent.builder()
                .eventId(eventId)
                .userId("replay-user")
                .channel("EMAIL")
                .recipient("replay@example.com")
                .message("Replay test")
                .build();
        ConsumerRecord<String, NotificationEvent> record =
                new ConsumerRecord<>("notification.email", 0, 0L, "replay-user", event);
        AtomicInteger acknowledgements = new AtomicInteger();
        Acknowledgment acknowledgment = acknowledgements::incrementAndGet;
        testDeliveryProvider.calls.set(0);

        emailConsumer.consume(record, acknowledgment);
        emailConsumer.consume(record, acknowledgment);

        assertThat(testDeliveryProvider.calls.get()).isEqualTo(1);
        assertThat(acknowledgements.get()).isEqualTo(2);
        assertThat(notificationRepository.findByEventId(eventId).orElseThrow().getStatus().name())
                .isEqualTo("DELIVERED");
    }

    private NotificationResponse submitAfter(CountDownLatch start, CountDownLatch ready,
                                              NotificationRequest request) throws InterruptedException {
        ready.countDown();
        if (!start.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting to start concurrent submission");
        }
        return notificationService.processNotification(request);
    }

    private OutboxEvent outbox(String eventId, String topic, String userId) {
        return OutboxEvent.builder()
                .eventId(eventId)
                .topic(topic)
                .messageKey(userId)
                .payload("{}")
                .published(false)
                .build();
    }

    @TestConfiguration
    static class TestBeans {
        @Bean
        TestDeliveryProvider testDeliveryProvider() {
            return new TestDeliveryProvider();
        }

        @Bean
        MeterRegistry testMeterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        static class TestDeliveryProvider implements NotificationDeliveryProvider {
            private final AtomicInteger calls = new AtomicInteger();

            @Override
            public void deliver(String channel, String recipient, String message, String idempotencyKey) {
                calls.incrementAndGet();
            }
        }
    }
}
