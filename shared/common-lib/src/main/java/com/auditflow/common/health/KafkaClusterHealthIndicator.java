package com.auditflow.common.health;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Can this service reach its Kafka cluster right now?
 *
 * <p>One {@code describeCluster} round trip, bounded by {@code timeout}. UP
 * when the broker answers with at least one node, DOWN otherwise. This is a
 * <em>readiness</em> question - "should traffic/work be sent here?" - and
 * it must never be wired into liveness: a broker outage would then mark
 * every task dead at once and ECS would replace them all into the same
 * outage (see the gateway's application.yml for the same argument about
 * Aurora).
 *
 * <p>Built from the same client properties the service's own producers and
 * consumers use, so under the {@code aws} profile it authenticates to MSK
 * with IAM exactly as they do - a probe that could reach a broker they
 * cannot would be lying.
 */
public final class KafkaClusterHealthIndicator implements HealthIndicator, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaClusterHealthIndicator.class);

    /** The one Kafka call the indicator makes, behind a seam so it can be tested without a broker. */
    public interface ClusterProbe extends AutoCloseable {
        Collection<Node> nodes(Duration timeout) throws Exception;

        @Override
        default void close() {
        }
    }

    private final ClusterProbe probe;
    private final Duration timeout;

    /**
     * @param clientProperties bootstrap servers plus any security settings,
     *                         typically {@code KafkaAdmin#getConfigurationProperties()}
     * @param timeout          how long one probe may take before it counts as DOWN
     */
    public KafkaClusterHealthIndicator(Map<String, Object> clientProperties, Duration timeout) {
        this(new AdminClusterProbe(clientProperties), timeout);
    }

    KafkaClusterHealthIndicator(ClusterProbe probe, Duration timeout) {
        this.probe = Objects.requireNonNull(probe, "probe");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive: " + timeout);
        }
    }

    @Override
    public Health health() {
        try {
            Collection<Node> nodes = probe.nodes(timeout);
            if (nodes == null || nodes.isEmpty()) {
                log.warn("Kafka health: cluster answered with no brokers");
                return Health.down().withDetail("brokers", 0).build();
            }
            return Health.up().withDetail("brokers", nodes.size()).build();
        } catch (Exception e) {
            // Class name only: the message can carry bootstrap addresses.
            log.warn("Kafka health: cluster unreachable within {} ({})", timeout, e.getClass().getSimpleName());
            return Health.down().withDetail("error", e.getClass().getSimpleName()).build();
        }
    }

    @Override
    public void close() throws Exception {
        probe.close();
    }

    /**
     * Lazily creates one {@link Admin} and keeps it: the client reconnects
     * on its own, so a failed probe does not need a new one. Creation is
     * deferred so a service whose broker is not up yet still starts (and
     * reports DOWN) instead of failing to boot.
     */
    static final class AdminClusterProbe implements ClusterProbe {

        private final Map<String, Object> properties;
        private Admin admin;

        AdminClusterProbe(Map<String, Object> clientProperties) {
            Map<String, Object> props = new HashMap<>(Objects.requireNonNull(clientProperties, "clientProperties"));
            // Visible broker-side as its own client, so a flood of probes is
            // never mistaken for a misbehaving producer or consumer.
            props.putIfAbsent("client.id", "auditflow-health");
            this.properties = Map.copyOf(props);
        }

        @Override
        public synchronized Collection<Node> nodes(Duration timeout) throws Exception {
            if (admin == null) {
                admin = Admin.create(properties);
            }
            int timeoutMs = (int) Math.min(Integer.MAX_VALUE, timeout.toMillis());
            return admin.describeCluster(new DescribeClusterOptions().timeoutMs(timeoutMs))
                    .nodes()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public synchronized void close() {
            if (admin != null) {
                admin.close(Duration.ofSeconds(5));
                admin = null;
            }
        }
    }
}
