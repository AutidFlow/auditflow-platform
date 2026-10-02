package com.auditflow.common.health;

import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaClusterHealthIndicatorTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    @Test
    void brokersAnsweringIsUpWithTheirCount() {
        KafkaClusterHealthIndicator indicator = new KafkaClusterHealthIndicator(
                timeout -> List.of(node(1), node(2), node(3)), TIMEOUT);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("brokers", 3);
    }

    @Test
    void aClusterWithNoBrokersIsDown() {
        KafkaClusterHealthIndicator indicator = new KafkaClusterHealthIndicator(timeout -> List.of(), TIMEOUT);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("brokers", 0);
    }

    @Test
    void anUnreachableClusterIsDownNamingOnlyTheExceptionClass() {
        KafkaClusterHealthIndicator indicator = new KafkaClusterHealthIndicator(timeout -> {
            throw new TimeoutException("b-1.msk.internal:9098 did not answer");
        }, TIMEOUT);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        // the message could carry a bootstrap address; only the class is reported
        assertThat(health.getDetails()).containsEntry("error", "TimeoutException");
        assertThat(health.getDetails().toString()).doesNotContain("msk.internal");
    }

    @Test
    void theProbeReceivesTheConfiguredTimeout() {
        Duration[] seen = new Duration[1];
        KafkaClusterHealthIndicator indicator = new KafkaClusterHealthIndicator(timeout -> {
            seen[0] = timeout;
            return List.of(node(1));
        }, Duration.ofMillis(750));

        indicator.health();

        assertThat(seen[0]).isEqualTo(Duration.ofMillis(750));
    }

    @Test
    void closingTheIndicatorClosesTheProbe() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        KafkaClusterHealthIndicator.ClusterProbe probe = new KafkaClusterHealthIndicator.ClusterProbe() {
            @Override
            public Collection<Node> nodes(Duration timeout) {
                return List.of(node(1));
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };

        new KafkaClusterHealthIndicator(probe, TIMEOUT).close();

        assertThat(closed).isTrue();
    }

    @Test
    void aNonPositiveTimeoutIsRejectedAtConstruction() {
        assertThatThrownBy(() -> new KafkaClusterHealthIndicator(timeout -> List.of(), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theRealProbeDefersClientCreationUntilFirstUse() {
        // Unresolvable bootstrap: constructing must not touch the network or
        // throw, or a service whose broker is late would fail to boot.
        KafkaClusterHealthIndicator.AdminClusterProbe probe = new KafkaClusterHealthIndicator.AdminClusterProbe(
                java.util.Map.of("bootstrap.servers", "localhost:1"));

        assertThatThrownBy(() -> probe.nodes(Duration.ofMillis(300))).isInstanceOf(Exception.class);
        probe.close();
    }

    private static Node node(int id) {
        return new Node(id, "broker-" + id, 9092);
    }
}
