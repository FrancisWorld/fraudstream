package dev.francisworld.fraudstream.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ManagementRoutesTest {

  @Test
  void healthReadinessAndMetricsAreServedWithoutCredentials() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    registry.counter("fraudstream.alerts").increment(3);
    AtomicBoolean ready = new AtomicBoolean(false);
    Router router = ManagementRoutes.create(registry, ready::get);

    assertThat(router.dispatch("GET", "/healthz", null, Map.of(), new byte[0]).status())
        .isEqualTo(200);
    assertThat(router.dispatch("GET", "/readyz", null, Map.of(), new byte[0]).status())
        .isEqualTo(503);
    ready.set(true);
    assertThat(router.dispatch("GET", "/readyz", null, Map.of(), new byte[0]).status())
        .isEqualTo(200);
    Response metrics = router.dispatch("GET", "/metrics", null, Map.of(), new byte[0]);
    assertThat(new String(metrics.body())).contains("fraudstream_alerts_total 3.0");
  }
}
