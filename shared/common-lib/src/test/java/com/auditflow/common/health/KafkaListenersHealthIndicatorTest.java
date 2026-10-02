package com.auditflow.common.health;

import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaListenersHealthIndicatorTest {

    @Test
    void everyContainerRunningIsUp() {
        KafkaListenersHealthIndicator indicator = new KafkaListenersHealthIndicator(
                () -> List.of(container("enrichment", true, true, false)));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("listeners", Map.of("enrichment", "running"));
    }

    @Test
    void oneStoppedAutoStartupContainerIsDownAndNamed() {
        KafkaListenersHealthIndicator indicator = new KafkaListenersHealthIndicator(() -> List.of(
                container("events", true, true, false),
                container("dead", false, true, false)));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("listeners", Map.of("events", "running", "dead", "stopped"));
    }

    @Test
    void aContainerTheServiceChoseNotToStartIsNotAFailure() {
        KafkaListenersHealthIndicator indicator = new KafkaListenersHealthIndicator(
                () -> List.of(container("manual", false, false, false)));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("listeners", Map.of("manual", "not-auto-started"));
    }

    @Test
    void aPausedContainerIsRunningAndSaysSo() {
        KafkaListenersHealthIndicator indicator = new KafkaListenersHealthIndicator(
                () -> List.of(container("paused", true, true, true)));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("listeners", Map.of("paused", "paused"));
    }

    @Test
    void noContainersAtAllIsUpWithAnEmptyList() {
        // Not something a restart fixes, so not a liveness failure; the
        // empty detail is what makes the misconfiguration visible.
        Health health = new KafkaListenersHealthIndicator(List::of).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("listeners", Map.of());
    }

    @Test
    void anAnonymousContainerGetsAPlaceholderId() {
        MessageListenerContainer container = container(null, true, true, false);

        Health health = new KafkaListenersHealthIndicator(() -> List.of(container)).health();

        assertThat(health.getDetails()).containsEntry("listeners", Map.of("<anonymous>", "running"));
    }

    private static MessageListenerContainer container(String id, boolean running, boolean autoStartup,
                                                      boolean paused) {
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(container.getListenerId()).thenReturn(id);
        when(container.isRunning()).thenReturn(running);
        when(container.isAutoStartup()).thenReturn(autoStartup);
        when(container.isContainerPaused()).thenReturn(paused);
        return container;
    }
}
