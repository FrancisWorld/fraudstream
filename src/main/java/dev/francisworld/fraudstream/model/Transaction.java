package dev.francisworld.fraudstream.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.regex.Pattern;

/**
 * A single card or account transaction as seen by the pipeline.
 *
 * <p>Money is carried as integer minor units (cents) to avoid floating point drift. Optional fields
 * are {@code null} when the producer did not supply them, for instance events written with the
 * first schema generation have no device or channel.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Transaction(
    String transactionId,
    String accountId,
    long amountMinor,
    String currency,
    String merchantId,
    String merchantCategory,
    String country,
    Double latitude,
    Double longitude,
    long eventTimeMs,
    String deviceId,
    String ipAddress,
    String channel) {

  private static final Pattern ID = Pattern.compile("[A-Za-z0-9_.:-]{1,64}");
  private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");

  public Transaction {
    requireId("transactionId", transactionId);
    requireId("accountId", accountId);
    requireId("merchantId", merchantId);
    if (amountMinor <= 0) {
      throw new IllegalArgumentException("amountMinor must be positive");
    }
    if (currency == null || !CURRENCY.matcher(currency).matches()) {
      throw new IllegalArgumentException("currency must be an ISO-4217 style code");
    }
    if (merchantCategory == null || merchantCategory.isBlank()) {
      throw new IllegalArgumentException("merchantCategory is required");
    }
    if ((latitude == null) != (longitude == null)) {
      throw new IllegalArgumentException("latitude and longitude must be given together");
    }
    if (latitude != null && (latitude < -90 || latitude > 90)) {
      throw new IllegalArgumentException("latitude out of range");
    }
    if (longitude != null && (longitude < -180 || longitude > 180)) {
      throw new IllegalArgumentException("longitude out of range");
    }
    if (eventTimeMs < 0) {
      throw new IllegalArgumentException("eventTimeMs must not be negative");
    }
    if (deviceId != null) {
      requireId("deviceId", deviceId);
    }
  }

  private static void requireId(String field, String value) {
    if (value == null || !ID.matcher(value).matches()) {
      throw new IllegalArgumentException(field + " must match " + ID.pattern());
    }
  }

  public boolean hasLocation() {
    return latitude != null && longitude != null;
  }

  public double amountMajor() {
    return amountMinor / 100.0;
  }
}
