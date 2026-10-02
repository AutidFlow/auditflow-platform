package com.auditflow.alerting;

import com.auditflow.common.health.KafkaClusterHealthIndicator;
import com.auditflow.common.health.KafkaListenersHealthIndicator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;

/**
 * Alerting's health indicators. Bean names are the indicator ids used in
 * {@code management.endpoint.health.group.*.include}:
 * {@code kafkaListenersHealthIndicator} is {@code kafkaListeners} (in
 * liveness - a stopped consumer is a defect of this process),
 * {@code kafkaClusterHealthIndicator} is {@code kafkaCluster} (in
 * readiness - a dependency, never a reason to kill the task).
 *
 * <p>The cluster probe is built from {@code alertingKafkaAdmin}'s properties, so it
 * reaches the broker exactly as this service's own Kafka clients do - same
 * bootstrap servers, same MSK IAM settings under the {@code aws} profile.
 */
@Configuration
public class HealthConfig {

    @Bean(destroyMethod = "close")
    public KafkaClusterHealthIndicator kafkaClusterHealthIndicator(
            KafkaAdmin alertingKafkaAdmin,
            @Value("${audit.health.kafka-timeout:3s}") Duration timeout) {
        return new KafkaClusterHealthIndicator(alertingKafkaAdmin.getConfigurationProperties(), timeout);
    }

    @Bean
    public KafkaListenersHealthIndicator kafkaListenersHealthIndicator(KafkaListenerEndpointRegistry registry) {
        return new KafkaListenersHealthIndicator(registry);
    }
}
