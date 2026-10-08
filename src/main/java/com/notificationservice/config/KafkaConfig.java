package com.notificationservice.config;

import com.notificationservice.entity.NotificationEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer/consumer configuration.
 *
 * Channel topics and the DLQ use three partitions so channel workloads scale
 * independently while preserving per-user ordering within each topic.
 */
@Configuration
@EnableKafka
@Slf4j
@DependsOn("embeddedKafkaBroker")
public class KafkaConfig {

    @Bean
    public NewTopic emailTopic(@Value("${kafka.topics.email}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic smsTopic(@Value("${kafka.topics.sms}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic pushTopic(@Value("${kafka.topics.push}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic dlqTopic(@Value("${kafka.topics.dlq}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topics.dlq}")
    private String dlqTopic;

    @Value("${kafka.consumer.retry.max-attempts:2}")
    private long maxRetryAttempts;

    @Value("${kafka.consumer.retry.backoff-ms:2000}")
    private long retryBackoffMs;

    // ── Producer ─────────────────────────────────────────────────────────────

    @Bean
    public ProducerFactory<String, NotificationEvent> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, NotificationEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ── Consumer ─────────────────────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, NotificationEvent> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.notificationservice.*");
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, NotificationEvent.class.getName());
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * Main container factory with retry + DLQ routing.
     * After 3 total attempts (1 initial + 2 retries), message goes to notification.dlq.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, NotificationEvent> consumerFactory,
            KafkaTemplate<String, NotificationEvent> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("Moving event to DLQ after {} retries | topic={} | error={}",
                            maxRetryAttempts, record.topic(), ex.getMessage());
                    return new TopicPartition(dlqTopic, record.partition() % 3);
                }
        );

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(retryBackoffMs, maxRetryAttempts)
        );

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
            log.warn("Retry attempt {}/{} | topic={} | key={} | error={}",
                    deliveryAttempt, maxRetryAttempts + 1,
                    record.topic(), record.key(), ex.getMessage())
        );

        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(3);
        return factory;
    }

    /**
     * Separate factory for DLQ consumer — no retry loop.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> dlqKafkaListenerContainerFactory(
            ConsumerFactory<String, NotificationEvent> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
