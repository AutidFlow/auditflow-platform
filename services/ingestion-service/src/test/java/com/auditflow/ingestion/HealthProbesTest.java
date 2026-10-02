package com.auditflow.ingestion;

import com.auditflow.ingestion.security.IngestTokenFilter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two probes, with no broker anywhere: liveness must be UP (the
 * process is serving), readiness must be DOWN (it cannot accept an event).
 * That split is the whole point - if a Kafka outage could fail liveness,
 * ECS would kill every ingestion task and replace it into the same outage.
 *
 * <p>No Docker on purpose. The broker-up half lives in
 * {@code EventIngestionIntegrationTest}.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                // tokens ON, so a probe that needed one would show up here as a 401
                "audit.ingestion.tokens=cust-1=tok-1",
                // a port nothing listens on: connection refused, fast
                "spring.kafka.bootstrap-servers=localhost:1",
                "spring.kafka.admin.auto-create=false",
                "audit.health.kafka-timeout=500ms"
        })
class HealthProbesTest {

    @Autowired
    private TestRestTemplate rest;

    @Test
    void livenessIsUpWithoutATokenAndWithoutKafka() {
        ResponseEntity<String> response = rest.getForEntity("/actuator/health/liveness", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo("{\"status\":\"UP\"}");
    }

    @Test
    void readinessIsDownWhileKafkaIsUnreachable() {
        ResponseEntity<String> response = rest.getForEntity("/actuator/health/readiness", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        // show-details: never - a probe learns the verdict, not the bootstrap servers
        assertThat(response.getBody()).isEqualTo("{\"status\":\"DOWN\"}");
    }

    @Test
    void theOverallHealthReportsDownTooAndStillHidesDetails() {
        ResponseEntity<String> response = rest.getForEntity("/actuator/health", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        // the aggregate names its groups, and nothing else: no components,
        // no error strings, no addresses
        assertThat(response.getBody()).isEqualTo("{\"status\":\"DOWN\",\"groups\":[\"liveness\",\"readiness\"]}");
    }

    @Test
    void otherActuatorEndpointsAreNotReachableEvenWithAValidToken() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(IngestTokenFilter.TOKEN_HEADER, "tok-1");

        for (String path : new String[]{"/actuator/env", "/actuator/beans", "/actuator/metrics"}) {
            ResponseEntity<String> response = rest.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), String.class);
            assertThat(response.getStatusCode()).as(path).isEqualTo(HttpStatus.NOT_FOUND);
        }
    }

    @Test
    void theEventEndpointStillWantsItsToken() {
        // GET rather than POST: the filter answers before routing either way,
        // and the JDK client cannot replay a streamed POST body after a 401.
        ResponseEntity<String> response = rest.getForEntity("/api/v1/events", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}
