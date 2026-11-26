package dev.francisworld.fraudstream.features;

/** Names of the numeric features produced by the pipeline stages and read by the rule DSL. */
public final class Features {

  public static final String VELOCITY_COUNT_1M = "velocity_count_1m";
  public static final String VELOCITY_COUNT_5M = "velocity_count_5m";
  public static final String VELOCITY_COUNT_1H = "velocity_count_1h";
  public static final String VELOCITY_SUM_5M = "velocity_sum_5m";
  public static final String VELOCITY_SUM_1H = "velocity_sum_1h";
  public static final String TRAVEL_SPEED_KMH = "travel_speed_kmh";
  public static final String TRAVEL_DISTANCE_KM = "travel_distance_km";
  public static final String MERCHANT_ZSCORE = "merchant_zscore";
  public static final String MERCHANT_TXN_COUNT = "merchant_txn_count";
  public static final String DEVICE_ACCOUNT_COUNT = "device_account_count";

  private Features() {}
}
