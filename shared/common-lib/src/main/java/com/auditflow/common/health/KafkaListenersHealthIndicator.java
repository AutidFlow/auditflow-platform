package com.auditflow.common.health;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Are this service's Kafka listeners still running?
 *
 * <p>A consumer service with a stopped listener container is a zombie: the
 * JVM is up, the port answers, and no events are being processed. That is
 * a defect of the process, not of a dependency - spring-kafka stops a
 * container for good on a fatal error such as a failed SASL handshake or a
 * missing topic ACL - and a restart is the right remedy. So unlike
 * {@link KafkaClusterHealthIndicator}, this one belongs in <em>liveness</em>.
 *
 * <p>Only containers marked auto-startup count: one the service chose not
 * to start, or explicitly stopped, is not a failure. A paused container is
 * running (pause is a deliberate, reversible state) and is reported as such.
 */
public final class KafkaListenersHealthIndicator implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(KafkaListenersHealthIndicator.class);

    private final Supplier<Collection<MessageListenerContainer>> containers;

    public KafkaListenersHealthIndicator(KafkaListenerEndpointRegistry registry) {
        this(Objects.requireNonNull(registry, "registry")::getAllListenerContainers);
    }

    KafkaListenersHealthIndicator(Supplier<Collection<MessageListenerContainer>> containers) {
        this.containers = Objects.requireNonNull(containers, "containers");
    }

    @Override
    public Health health() {
        Map<String, String> states = new LinkedHashMap<>();
        boolean allRunning = true;
        for (MessageListenerContainer container : containers.get()) {
            String id = container.getListenerId() == null ? "<anonymous>" : container.getListenerId();
            String state;
            if (container.isRunning()) {
                state = container.isContainerPaused() ? "paused" : "running";
            } else if (!container.isAutoStartup()) {
                state = "not-auto-started";
            } else {
                state = "stopped";
                allRunning = false;
            }
            states.put(id, state);
        }
        if (!allRunning) {
            log.warn("Kafka listeners health: a listener container has stopped: {}", states);
        }
        Health.Builder health = allRunning ? Health.up() : Health.down();
        return health.withDetail("listeners", states).build();
    }
}
