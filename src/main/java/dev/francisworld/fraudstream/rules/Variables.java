package dev.francisworld.fraudstream.rules;

import dev.francisworld.fraudstream.features.Features;
import dev.francisworld.fraudstream.model.EnrichedTransaction;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/** The closed catalogue of variables a rule may reference. */
public final class Variables {

  /** A named, typed accessor into an enriched transaction. */
  public record Variable(
      String name, ValueType type, Function<EnrichedTransaction, Object> accessor) {
    Object read(EnrichedTransaction tx) {
      return accessor.apply(tx);
    }
  }

  private static final Map<String, Variable> CATALOG = new LinkedHashMap<>();

  static {
    number("amount", tx -> tx.transaction().amountMajor());
    number("hour_of_day", tx -> (double) hourOfDay(tx.transaction().eventTimeMs()));
    for (String feature :
        new String[] {
          Features.VELOCITY_COUNT_1M,
          Features.VELOCITY_COUNT_5M,
          Features.VELOCITY_COUNT_1H,
          Features.VELOCITY_SUM_5M,
          Features.VELOCITY_SUM_1H,
          Features.TRAVEL_SPEED_KMH,
          Features.TRAVEL_DISTANCE_KM,
          Features.MERCHANT_ZSCORE,
          Features.MERCHANT_TXN_COUNT,
          Features.DEVICE_ACCOUNT_COUNT
        }) {
      number(feature, tx -> tx.feature(feature));
    }
    text("currency", tx -> tx.transaction().currency());
    text("country", tx -> tx.transaction().country());
    text("merchant_id", tx -> tx.transaction().merchantId());
    text("merchant_category", tx -> tx.transaction().merchantCategory());
    text("channel", tx -> tx.transaction().channel());
    text("device_id", tx -> tx.transaction().deviceId());
  }

  private Variables() {}

  private static void number(String name, Function<EnrichedTransaction, Object> accessor) {
    CATALOG.put(name, new Variable(name, ValueType.NUMBER, accessor));
  }

  private static void text(String name, Function<EnrichedTransaction, String> accessor) {
    CATALOG.put(
        name,
        new Variable(
            name,
            ValueType.STRING,
            tx -> {
              String value = accessor.apply(tx);
              return value == null ? "" : value;
            }));
  }

  private static int hourOfDay(long epochMs) {
    return Instant.ofEpochMilli(epochMs).atZone(ZoneOffset.UTC).getHour();
  }

  public static Variable lookup(String name) {
    return CATALOG.get(name);
  }

  public static Map<String, Variable> all() {
    return Collections.unmodifiableMap(CATALOG);
  }
}
