package com.auditflow.alerting.dispatch;

import com.auditflow.common.model.AuditEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** The feed: enriched events from enrichment-service, one dispatch each. */
@Component
public class EnrichedEventListener {

    /** Enriched events by {@code result}: {@code processed} here, {@code dropped} in the error handler. */
    public static final String EVENTS = "auditflow.alerting.events";

    private final AlertDispatcher dispatcher;
    private final Counter processed;

    public EnrichedEventListener(AlertDispatcher dispatcher, MeterRegistry registry) {
        this.dispatcher = dispatcher;
        this.processed = Counter.builder(EVENTS)
                .description("Enriched events consumed, matched against rules or given up on")
                .tag("result", "processed")
                .register(registry);
    }

    @KafkaListener(topics = "${audit.alerting.enriched-topic}")
    public void onEnrichedEvent(AuditEvent event) {
        dispatcher.dispatch(event);
        processed.increment();
    }
}
