package com.auditflow.enrichment.adapters;

import com.auditflow.common.interfaces.DataSink;
import com.auditflow.common.interfaces.EventProcessor;
import com.auditflow.common.model.AuditEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Consumes raw events from ingestion-service, runs them through the
 * ordered enrichment pipeline, and fans the result out to every
 * configured {@link DataSink}.
 *
 * <p>Sink order is deliberate and enforced here rather than assumed:
 * evidence to S3, then the queryable row, then the enriched topic that
 * pages someone. A failure part-way through is retried by the container
 * (see {@link KafkaConsumerConfig}), and every sink is idempotent, so the
 * repeat is harmless - but an alert must never precede the evidence.
 */
@Component
public class KafkaConsumerAdapter {

    /** Events that made it through every sink, by {@code result}. */
    public static final String EVENTS = "auditflow.enrichment.events";
    /** One write to one sink, by {@code sink}; only successful writes are timed. */
    public static final String SINK_WRITES = "auditflow.enrichment.sink.writes";
    /** Writes that threw, by {@code sink}. The container retries the whole event. */
    public static final String SINK_FAILURES = "auditflow.enrichment.sink.failures";

    private final List<EventProcessor> processors;
    private final List<DataSink> sinks;
    private final MeterRegistry registry;
    private final Counter processed;

    public KafkaConsumerAdapter(List<EventProcessor> processors, List<DataSink> sinks, MeterRegistry registry) {
        this.registry = registry;
        this.processed = Counter.builder(EVENTS)
                .description("Events enriched and written to every sink, or dead-lettered")
                .tag("result", "processed")
                .register(registry);
        this.processors = processors.stream()
                .sorted(Comparator.comparingInt(EventProcessor::order))
                .toList();
        // Spring usually injects @Order-annotated beans in order, but sorting
        // here makes the guarantee the adapter's own rather than the
        // container's - and lets a test hand them over shuffled.
        List<DataSink> ordered = new ArrayList<>(sinks);
        AnnotationAwareOrderComparator.sort(ordered);
        this.sinks = List.copyOf(ordered);
    }

    /** Visible for testing: the sinks in the order they are written to. */
    List<DataSink> sinks() {
        return sinks;
    }

    @KafkaListener(topics = "${audit.ingestion.topic}")
    public void onMessage(AuditEvent rawEvent) {
        AuditEvent enriched = rawEvent;
        for (EventProcessor processor : processors) {
            enriched = processor.process(enriched);
        }
        for (DataSink sink : sinks) {
            write(sink, enriched);
        }
        processed.increment();
    }

    /**
     * Timed per sink so a slow S3 or a saturated Aurora shows up as that
     * sink's latency, not as a vague "enrichment is slow". A failure is
     * counted and rethrown: the container owns the retry and, after the
     * back-off budget, the dead-letter (see {@link KafkaConsumerConfig}).
     */
    private void write(DataSink sink, AuditEvent event) {
        Timer.Sample sample = Timer.start(registry);
        try {
            sink.write(event);
        } catch (RuntimeException e) {
            registry.counter(SINK_FAILURES, "sink", sink.sinkName()).increment();
            throw e;
        }
        sample.stop(Timer.builder(SINK_WRITES)
                .description("Successful writes of one enriched event to one sink")
                .tag("sink", sink.sinkName())
                .register(registry));
    }
}
