package com.auditflow.ingestion;

import com.auditflow.common.health.KafkaClusterHealthIndicator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;

/**
 * Ingestion's health indicators. The bean name is the indicator id:
 * {@code kafkaClusterHealthIndicator} is {@code kafkaCluster} in
 * {@code management.endpoint.health.group.readiness.include}.
 *
 * <p>Built from {@code auditKafkaAdmin}'s properties so the probe reaches
 * the broker exactly as the producer does - same bootstrap servers, same
 * MSK IAM settings under the {@code aws} profile. No listeners here
 * (ingestion only produces), so no listener indicator.
 */
@Configuration
public class HealthConfig {

    @Bean(destroyMethod = "close")
    public KafkaClusterHealthIndicator kafkaClusterHealthIndicator(
            KafkaAdmin auditKafkaAdmin,
            @Value("${audit.health.kafka-timeout:3s}") Duration timeout) {
        return new KafkaClusterHealthIndicator(auditKafkaAdmin.getConfigurationProperties(), timeout);
    }
}
