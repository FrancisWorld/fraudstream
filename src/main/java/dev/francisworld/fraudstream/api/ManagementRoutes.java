package dev.francisworld.fraudstream.api;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.Map;
import java.util.function.BooleanSupplier;

/** Unauthenticated liveness, readiness and metrics, served on a separate port. */
public final class ManagementRoutes {

  private ManagementRoutes() {}

  public static Router create(PrometheusMeterRegistry registry, BooleanSupplier ready) {
    Router router = new Router(null, null, null, null);
    router.route("GET", "/healthz", null, r -> Response.json(200, Map.of("status", "up")));
    router.route(
        "GET",
        "/readyz",
        null,
        r ->
            ready.getAsBoolean()
                ? Response.json(200, Map.of("status", "ready"))
                : Response.json(503, Map.of("status", "starting")));
    router.route(
        "GET",
        "/metrics",
        null,
        r -> Response.text(200, registry.scrape(), "text/plain; version=0.0.4"));
    return router;
  }
}
