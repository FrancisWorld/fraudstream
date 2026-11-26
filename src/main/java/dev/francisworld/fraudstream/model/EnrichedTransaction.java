package dev.francisworld.fraudstream.model;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/** A transaction plus the numeric features computed by the stateful stages. */
public record EnrichedTransaction(Transaction transaction, Map<String, Double> features) {

  public EnrichedTransaction {
    features = Collections.unmodifiableMap(new TreeMap<>(features));
  }

  public static EnrichedTransaction of(Transaction transaction) {
    return new EnrichedTransaction(transaction, Map.of());
  }

  public EnrichedTransaction with(String name, double value) {
    Map<String, Double> copy = new TreeMap<>(features);
    copy.put(name, value);
    return new EnrichedTransaction(transaction, copy);
  }

  public EnrichedTransaction withAll(Map<String, Double> more) {
    Map<String, Double> copy = new TreeMap<>(features);
    copy.putAll(more);
    return new EnrichedTransaction(transaction, copy);
  }

  public double feature(String name) {
    return features.getOrDefault(name, 0.0);
  }
}
