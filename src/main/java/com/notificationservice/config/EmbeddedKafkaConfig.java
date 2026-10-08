package com.notificationservice.config;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;

/**
 * Starts an in-process Kafka broker for local and portfolio runs, so Kafka
 * does not need to be installed separately or started with Docker.
 */
@Configuration
@Slf4j
public class EmbeddedKafkaConfig {

    private static final int BROKER_PORT = 19092;
    private static final int PARTITIONS = 3;
    private EmbeddedKafkaBroker broker;

    @Bean(name = "embeddedKafkaBroker")
    public EmbeddedKafkaBroker embeddedKafkaBroker() {
        broker = new EmbeddedKafkaZKBroker(
                1,
                false,
                PARTITIONS,
                "notification.email",
                "notification.sms",
                "notification.push",
                "notification.dlq"
        );
        broker.kafkaPorts(BROKER_PORT);
        broker.afterPropertiesSet();
        log.info("Embedded Kafka started on port {}", BROKER_PORT);
        return broker;
    }

    @PreDestroy
    public void stopBroker() {
        if (broker != null) {
            broker.destroy();
        }
    }
}
