package com.auditflow.common.metrics;

import io.micrometer.cloudwatch2.CloudWatchConfig;
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
}
