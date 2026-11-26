package dev.francisworld.fraudstream.model;

/** Alert severity derived from the aggregate rule score (0-100). */
public enum Severity {
  LOW,
  MEDIUM,
  HIGH,
  CRITICAL;

  public static Severity forScore(int score) {
    if (score >= 90) {
      return CRITICAL;
    }
    if (score >= 70) {
      return HIGH;
    }
    if (score >= 40) {
      return MEDIUM;
    }
    return LOW;
  }
}
