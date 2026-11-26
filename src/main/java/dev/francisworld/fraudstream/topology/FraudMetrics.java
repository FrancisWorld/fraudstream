package dev.francisworld.fraudstream.topology;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** Counters emitted by the topology; the registry is injected so tests can read them back. */
public final class FraudMetrics {

  private final MeterRegistry registry;
  private final Counter decoded;
  private final Counter duplicates;
  private final Counter late;
  private final Counter alerts;
  private final Counter ruleErrors;
  private final Counter noRules;

  public FraudMetrics(MeterRegistry registry) {
    this.registry = registry;
    this.decoded = Counter.builder("fraudstream.transactions.decoded").register(registry);
    this.duplicates = Counter.builder("fraudstream.transactions.duplicates").register(registry);
    this.late = Counter.builder("fraudstream.transactions.late").register(registry);
    this.alerts = Counter.builder("fraudstream.alerts").register(registry);
    this.ruleErrors = Counter.builder("fraudstream.rules.evaluation.errors").register(registry);
    this.noRules = Counter.builder("fraudstream.transactions.unscored").register(registry);
  }

  public MeterRegistry registry() {
    return registry;
  }

  void decoded() {
    decoded.increment();
  }

  void deadLetter(String source, String reason) {
    Counter.builder("fraudstream.dead.letters")
        .tag("source", source)
        .tag("reason", reason)
        .register(registry)
        .increment();
  }

  void duplicate() {
    duplicates.increment();
  }

  void late() {
    late.increment();
  }

  void alert() {
    alerts.increment();
  }

  void ruleError(int count) {
    ruleErrors.increment(count);
  }

  void unscored() {
    noRules.increment();
  }

  void rulesReload(String result) {
    Counter.builder("fraudstream.rules.reloads")
        .tag("result", result)
        .register(registry)
        .increment();
  }

  void caseCommand(String result) {
    Counter.builder("fraudstream.case.commands")
        .tag("result", result)
        .register(registry)
        .increment();
  }
}
