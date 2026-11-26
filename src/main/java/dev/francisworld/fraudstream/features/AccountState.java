package dev.francisworld.fraudstream.features;

import java.util.List;

/**
 * Recent transactions of one account ordered by event time, the state behind velocity and
 * impossible-travel features.
 */
public record AccountState(List<Entry> entries) {

  /** One remembered transaction. */
  public record Entry(String id, long ts, long amountMinor, Double lat, Double lon) {
    boolean located() {
      return lat != null && lon != null;
    }
  }

  public AccountState {
    entries = List.copyOf(entries);
  }

  public static AccountState empty() {
    return new AccountState(List.of());
  }
}
