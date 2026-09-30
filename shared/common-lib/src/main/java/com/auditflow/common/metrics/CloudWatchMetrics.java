package com.auditflow.common.metrics;

import io.micrometer.cloudwatch2.CloudWatchConfig;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.Clock;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

import java.time.Duration;
import java.util.Map;

/**
 * The one way every service builds its CloudWatch registry. Spring Boot
 * auto-configures registries for a dozen vendors but not this one (that
 * lives in Spring Cloud AWS, which nothing here depends on), so each
 * service declares the bean itself, from here, and Boot then treats it
 * like any other: the {@code management.metrics.enable.*} allow-list in
 * the aws profile is applied to it, the JVM and Kafka binders register
 * against it, and the simple in-memory registry backs off.
 *
 * <p>Plain code, no Spring, like the health indicators: common-lib holds
 * the behaviour, the service holds the small {@code @Configuration}.
 */
public final class CloudWatchMetrics {

    private CloudWatchMetrics() {
    }

    /**
     * @param namespace CloudWatch namespace, {@code AuditFlow/<env name>};
     *                  the infra repo's alarms look here, so it is required
     *                  rather than defaulted
     * @param step      how often each task publishes; 1 minute matches the
     *                  alarms' period, and less would only cost more
     */
    public static CloudWatchConfig config(String namespace, Duration step) {
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("CloudWatch metrics namespace is required "
                    + "(audit.metrics.cloudwatch.namespace / AUDIT_METRICS_NAMESPACE)");
        }
        Map<String, String> settings = Map.of(
                "cloudwatch.namespace", namespace,
                "cloudwatch.step", step.toString());
        return settings::get;
    }

    public static CloudWatchMeterRegistry registry(CloudWatchConfig config, CloudWatchAsyncClient client) {
        return new CloudWatchMeterRegistry(config, Clock.SYSTEM, client);
    }
}
