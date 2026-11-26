package dev.francisworld.fraudstream.model;

import java.util.Map;
import java.util.TreeMap;

/** Accounts observed on one device, each with the last event time it was seen. */
public record DeviceLinks(String deviceId, Map<String, Long> accountLastSeenMs) {

  public DeviceLinks {
    accountLastSeenMs = new TreeMap<>(accountLastSeenMs);
  }
}
