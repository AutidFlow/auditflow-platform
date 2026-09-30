package com.auditflow.enrichment;

import com.auditflow.common.metrics.CloudWatchMetrics;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

import java.time.Duration;

/**
 * Enrichment's metrics export, on only when {@code audit.metrics.cloudwatch.enabled}
 * is - the aws profile. Anywhere else Boot's in-memory registry stands in
 * and the meters are still there for tests and the local actuator.
 *
 * <p>The client takes region and credentials the same way the other AWS
 * clients on the task do: {@code AWS_REGION} and the task role. The task
 * role needs {@code cloudwatch:PutMetricData}; the ecs module grants it.
 */
@Configuration
@ConditionalOnProperty(prefix = "audit.metrics.cloudwatch", name = "enabled", havingValue = "true")
public class MetricsConfig {

    @Bean(destroyMethod = "close")
    public CloudWatchAsyncClient cloudWatchAsyncClient() {
        return CloudWatchAsyncClient.create();
    }

    @Bean
    public CloudWatchMeterRegistry cloudWatchMeterRegistry(
            CloudWatchAsyncClient cloudWatchAsyncClient,
            @Value("${audit.metrics.cloudwatch.namespace:}") String namespace,
            @Value("${audit.metrics.cloudwatch.step:1m}") Duration step) {
        return CloudWatchMetrics.registry(CloudWatchMetrics.config(namespace, step), cloudWatchAsyncClient);
    }

    /** The consumer-lag alarm names the group, not the client; see {@link CloudWatchMetrics#consumerLagByGroup}. */
    @Bean
    public MeterFilter consumerLagByGroup(@Value("${spring.kafka.consumer.group-id}") String consumerGroup) {
        return CloudWatchMetrics.consumerLagByGroup(consumerGroup);
    }
}
