package com.auditflow.common.metrics;

import io.micrometer.cloudwatch2.CloudWatchConfig;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
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

    /** The Kafka client's per-consumer backlog gauge, the one client meter that is exported. */
    public static final String CONSUMER_LAG_MAX = "kafka.consumer.fetch.manager.records.lag.max";
    /** The one dimension the exported gauge keeps. */
    public static final String CONSUMER_GROUP_TAG = "consumer.group";

    /**
     * Gives the lag gauge a dimension an alarm can name. The Kafka binder
     * tags it with {@code client.id}, {@code kafka.version} and
     * {@code spring.id} - values that change with every library upgrade
     * and every task, and a CloudWatch alarm has to match a metric's
     * dimensions exactly. This drops those and tags it with the consumer
     * group instead, which is what the alarm is about.
     *
     * <p>Assumes one consumer per task ({@code concurrency} 1, the
     * default), so nothing collides; with more, Micrometer would keep
     * the first gauge and the others would go unreported. Raise the
     * concurrency and this needs a partition or client tag back.
     */
    public static MeterFilter consumerLagByGroup(String consumerGroup) {
        return new MeterFilter() {
            @Override
            public Meter.Id map(Meter.Id id) {
                if (!CONSUMER_LAG_MAX.equals(id.getName())) {
                    return id;
                }
                return id.replaceTags(Tags.of(CONSUMER_GROUP_TAG, consumerGroup));
            }
        };
    }
}
