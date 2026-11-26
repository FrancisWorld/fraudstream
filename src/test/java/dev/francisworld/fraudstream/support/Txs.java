package dev.francisworld.fraudstream.support;

import dev.francisworld.fraudstream.model.Transaction;
import java.time.Instant;

/** Compact builder for test transactions. */
public final class Txs {

  public static final Instant T0 = Instant.parse("2026-03-02T12:00:00Z");

  private String id = "t-1";
  private String account = "acc-1";
  private long amountMinor = 2_500;
  private String merchant = "m-1";
  private String category = "5411";
  private String country = "BR";
  private Double lat;
  private Double lon;
  private long eventTimeMs = T0.toEpochMilli();
  private String device;

  private Txs() {}

  public static Txs tx(String id) {
    Txs t = new Txs();
    t.id = id;
    return t;
  }

  public Txs account(String value) {
    this.account = value;
    return this;
  }

  public Txs amount(double major) {
    this.amountMinor = Math.round(major * 100);
    return this;
  }

  public Txs merchant(String value) {
    this.merchant = value;
    return this;
  }

  public Txs country(String value) {
    this.country = value;
    return this;
  }

  public Txs at(double lat, double lon) {
    this.lat = lat;
    this.lon = lon;
    return this;
  }

  public Txs atSecond(long secondsAfterT0) {
    this.eventTimeMs = T0.toEpochMilli() + secondsAfterT0 * 1_000;
    return this;
  }

  public Txs atMinute(long minutesAfterT0) {
    return atSecond(minutesAfterT0 * 60);
  }

  public Txs device(String value) {
    this.device = value;
    return this;
  }

  public Transaction build() {
    return new Transaction(
        id,
        account,
        amountMinor,
        "BRL",
        merchant,
        category,
        country,
        lat,
        lon,
        eventTimeMs,
        device,
        device == null ? null : "203.0.113.7",
        device == null ? null : "mobile");
  }
}
