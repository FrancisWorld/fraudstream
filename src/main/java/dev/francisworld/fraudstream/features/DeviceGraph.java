package dev.francisworld.fraudstream.features;

import dev.francisworld.fraudstream.model.DeviceLinks;
import java.util.Map;
import java.util.TreeMap;

/** Maintains the account-to-device edges inside a sliding event-time window. */
public final class DeviceGraph {

  static final int MAX_ACCOUNTS = 1_000;

  private final long windowMs;

  public DeviceGraph(long windowMs) {
    this.windowMs = windowMs;
  }

  public DeviceLinks observe(DeviceLinks previous, String accountId, long eventTimeMs) {
    Map<String, Long> seen = new TreeMap<>(previous.accountLastSeenMs());
    seen.merge(accountId, eventTimeMs, Math::max);
    long newest = seen.values().stream().mapToLong(Long::longValue).max().orElse(eventTimeMs);
    long floor = newest - windowMs;
    seen.entrySet().removeIf(e -> e.getValue() < floor && !e.getKey().equals(accountId));
    while (seen.size() > MAX_ACCOUNTS) {
      String oldest =
          seen.entrySet().stream()
              .min(Map.Entry.comparingByValue())
              .map(Map.Entry::getKey)
              .orElseThrow();
      seen.remove(oldest);
    }
    return new DeviceLinks(previous.deviceId(), seen);
  }
}
