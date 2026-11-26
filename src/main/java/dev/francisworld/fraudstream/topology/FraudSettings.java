package dev.francisworld.fraudstream.topology;

import java.time.Duration;

/** Tunable behaviour of the topology. */
public record FraudSettings(Duration grace, Duration deviceWindow, String activeRuleSet) {

  public FraudSettings {
    if (grace.isNegative()) {
      throw new IllegalArgumentException("grace must not be negative");
    }
    if (deviceWindow.isNegative() || deviceWindow.isZero()) {
      throw new IllegalArgumentException("deviceWindow must be positive");
    }
  }

  public static FraudSettings defaults() {
    return new FraudSettings(Duration.ofMinutes(5), Duration.ofHours(24), "default");
  }
}
