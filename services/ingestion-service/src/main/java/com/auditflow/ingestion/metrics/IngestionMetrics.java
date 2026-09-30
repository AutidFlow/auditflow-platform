package com.auditflow.ingestion.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * What happened to each event a source posted. One counter,
 * {@code auditflow.ingestion.events}, one {@code result} tag - five
 * CloudWatch metrics in total:
 *
 * <ul>
 *   <li>{@code accepted} - the broker acknowledged it; the source got 202.</li>
 *   <li>{@code publish_failed} - the broker did not, within the timeout;
 *       the source got 503 + Retry-After. The alarm is on this one: a
 *       sustained rate means sources are holding checkpoints and the
 *       pipeline is not taking evidence in.</li>
 *   <li>{@code tenant_mismatch} - authenticated, but claiming another
 *       customer's id. A source trying to write someone else's trail.</li>
 *   <li>{@code invalid} - malformed or failed a domain check (400).</li>
 *   <li>{@code rate_limited} - is <em>not</em> here: the shared filter
 *       answers 429 before the controller; see {@code RateLimitFilter}.</li>
 * </ul>
 *
 * <p>No {@code customerId} tag on purpose. Per-tenant volume is a
 * question the gateway's operator view already answers from Aurora, and
 * a tag that grows with the customer list is a CloudWatch bill that grows
 * with it.
 */
@Component
public class IngestionMetrics {

    public static final String EVENTS = "auditflow.ingestion.events";

    private final Counter accepted;
    private final Counter publishFailed;
    private final Counter tenantMismatch;
    private final Counter invalid;

    public IngestionMetrics(MeterRegistry registry) {
        this.accepted = counter(registry, "accepted");
        this.publishFailed = counter(registry, "publish_failed");
        this.tenantMismatch = counter(registry, "tenant_mismatch");
        this.invalid = counter(registry, "invalid");
    }

    private static Counter counter(MeterRegistry registry, String result) {
        return Counter.builder(EVENTS)
                .description("Audit events posted to the ingestion endpoint, by outcome")
                .tag("result", result)
                .register(registry);
    }

    public void accepted() {
        accepted.increment();
    }

    public void publishFailed() {
        publishFailed.increment();
    }

    public void tenantMismatch() {
        tenantMismatch.increment();
    }

    public void invalid() {
        invalid.increment();
    }
}
