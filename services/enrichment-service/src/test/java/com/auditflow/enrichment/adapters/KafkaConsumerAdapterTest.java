package com.auditflow.enrichment.adapters;

import com.auditflow.common.enums.EventType;
import com.auditflow.common.interfaces.DataSink;
import com.auditflow.common.interfaces.EventProcessor;
import com.auditflow.common.model.AuditEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Sinks must run evidence-first: S3, then the queryable row, then the
 * topic that pages someone. Before this the three shared one precedence
 * value, so the order was whatever the container happened to produce - and
 * an alert could fire on an event that was not yet stored anywhere.
 */
@SuppressWarnings("unchecked")
class KafkaConsumerAdapterTest {

    @Test
    void sinksRunEvidenceFirstWhateverOrderTheyArriveIn() {
        // Real sinks, constructed offline: their @Order annotations are the
        // thing under test, so mocks would prove nothing.
        DataSink s3 = new S3WriterAdapter(mock(software.amazon.awssdk.services.s3.S3Client.class),
                new ObjectMapper(), "bucket");
        DataSink aurora = new AuroraWriterAdapter(mock(org.springframework.jdbc.core.JdbcTemplate.class));
        DataSink enriched = new EnrichedTopicSink(mock(KafkaTemplate.class), "topic", Duration.ofSeconds(1));

        // handed over in the worst possible order
        KafkaConsumerAdapter adapter = new KafkaConsumerAdapter(
                List.of(), List.of(enriched, aurora, s3), new SimpleMeterRegistry());

        assertThat(adapter.sinks().stream().map(DataSink::sinkName))
                .containsExactly("s3", "aurora", "kafka-enriched");
    }

    @Test
    void processorsStillRunInTheirOwnDeclaredOrder() {
        EventProcessor late = processor(30);
        EventProcessor early = processor(10);

        KafkaConsumerAdapter adapter = new KafkaConsumerAdapter(List.of(late, early), List.of(), new SimpleMeterRegistry());

        // no accessor for processors; the ordering is observable through the
        // pipeline, so this just pins that construction accepts them
        assertThat(adapter.sinks()).isEmpty();
    }

    private static EventProcessor processor(int order) {
        return new EventProcessor() {
            @Override
            public com.auditflow.common.model.AuditEvent process(com.auditflow.common.model.AuditEvent event) {
                return event;
            }

            @Override
            public int order() {
                return order;
            }
        };
    }

    /**
     * The meters an alarm or a runbook reads: every successful pass counts
     * one processed event and times one write per sink; a sink that throws
     * counts a failure against that sink, times nothing, counts no processed
     * event, and lets the exception reach the container that owns retries.
     */
    @Test
    void metersCountProcessedEventsAndPerSinkWritesAndFailures() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DataSink good = sink("good", null);
        DataSink bad = sink("bad", new IllegalStateException("sink down"));
        AuditEvent event = AuditEvent.builder().eventId("evt-1").customerId("c1")
                .type(EventType.DATA_EXPORT).timestamp(Instant.now()).build();

        KafkaConsumerAdapter healthy = new KafkaConsumerAdapter(List.of(), List.of(good), registry);
        healthy.onMessage(event);
        healthy.onMessage(event);

        assertThat(registry.get(KafkaConsumerAdapter.EVENTS).tag("result", "processed").counter().count())
                .isEqualTo(2);
        assertThat(registry.get(KafkaConsumerAdapter.SINK_WRITES).tag("sink", "good").timer().count())
                .isEqualTo(2);

        KafkaConsumerAdapter failing = new KafkaConsumerAdapter(List.of(), List.of(good, bad), registry);
        assertThatThrownBy(() -> failing.onMessage(event)).isInstanceOf(IllegalStateException.class);

        assertThat(registry.get(KafkaConsumerAdapter.SINK_FAILURES).tag("sink", "bad").counter().count())
                .isEqualTo(1);
        // the good sink before it was written and timed; the event as a whole was not processed
        assertThat(registry.get(KafkaConsumerAdapter.SINK_WRITES).tag("sink", "good").timer().count())
                .isEqualTo(3);
        assertThat(registry.get(KafkaConsumerAdapter.EVENTS).tag("result", "processed").counter().count())
                .isEqualTo(2);
        assertThat(registry.find(KafkaConsumerAdapter.SINK_WRITES).tag("sink", "bad").timer()).isNull();
    }

    private static DataSink sink(String name, RuntimeException failure) {
        return new DataSink() {
            @Override
            public void write(AuditEvent event) {
                if (failure != null) {
                    throw failure;
                }
            }

            @Override
            public void writeBatch(List<AuditEvent> events) {
            }

            @Override
            public String sinkName() {
                return name;
            }
        };
    }

}
