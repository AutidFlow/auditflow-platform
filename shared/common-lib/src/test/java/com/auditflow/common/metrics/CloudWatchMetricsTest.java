package com.auditflow.common.metrics;

import io.micrometer.cloudwatch2.CloudWatchConfig;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudWatchMetricsTest {

    @Test
    void namespaceAndStepComeFromTheCaller() {
        CloudWatchConfig config = CloudWatchMetrics.config("AuditFlow/auditflow-dev", Duration.ofMinutes(1));

        assertThat(config.namespace()).isEqualTo("AuditFlow/auditflow-dev");
        assertThat(config.step()).isEqualTo(Duration.ofMinutes(1));
        assertThat(config.enabled()).isTrue();
        // everything not set here is Micrometer's default, not a silent null
        assertThat(config.batchSize()).isEqualTo(CloudWatchConfig.MAX_BATCH_SIZE);
    }

    @Test
    void aMissingNamespaceFailsAtStartupNotAtFirstPublish() {
        // An empty AUDIT_METRICS_NAMESPACE would otherwise become a task that
        // starts, passes health, and publishes into a namespace nobody alarms on.
        assertThatThrownBy(() -> CloudWatchMetrics.config("", Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AUDIT_METRICS_NAMESPACE");
        assertThatThrownBy(() -> CloudWatchMetrics.config(null, Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theLagGaugeKeepsOnlyTheConsumerGroupAndOtherMetersKeepTheirTags() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.config().meterFilter(CloudWatchMetrics.consumerLagByGroup("enrichment-service"));

        Gauge.builder(CloudWatchMetrics.CONSUMER_LAG_MAX, () -> 42)
                .tags("client.id", "consumer-enrichment-service-1", "kafka.version", "3.7.1",
                        "spring.id", "auditEventConsumerFactory.consumer-enrichment-service-1")
                .register(registry);
        registry.counter("auditflow.enrichment.events", "result", "processed").increment();

        Meter.Id lag = registry.get(CloudWatchMetrics.CONSUMER_LAG_MAX).gauge().getId();
        assertThat(lag.getTags()).containsExactly(Tag.of("consumer.group", "enrichment-service"));

        Meter.Id events = registry.get("auditflow.enrichment.events").counter().getId();
        assertThat(events.getTags()).containsExactly(Tag.of("result", "processed"));
    }

}
