package dev.francisworld.fraudstream.model;

import java.util.List;
import java.util.Map;

/**
 * Emitted when at least one rule matches. The id is deterministic (transaction plus rule set
 * version) so replays and at-least-once redelivery never create duplicate cases.
 */
public record Alert(
    String alertId,
    String transactionId,
    String accountId,
    long eventTimeMs,
    long ruleSetVersion,
    int totalScore,
    Severity severity,
    List<RuleHit> hits,
    long amountMinor,
    String currency,
    String merchantId,
    String deviceId,
    Map<String, Double> features) {

  public Alert {
    hits = List.copyOf(hits);
    features = Map.copyOf(features);
  }

  public static String idFor(String transactionId, long ruleSetVersion) {
    return transactionId + "@v" + ruleSetVersion;
  }
}
