package dev.francisworld.fraudstream.features;

/**
 * Running mean and variance (Welford) of log-amounts for one merchant. Log space makes the
 * heavy-tailed payment amounts roughly symmetric, so a z-score is meaningful.
 */
public record MerchantStats(long count, double mean, double m2) {

  static final long MIN_SAMPLES = 30;
  static final double MIN_STD = 0.1;
  static final double BASELINE_Z_LIMIT = 4.0;

  public static MerchantStats empty() {
    return new MerchantStats(0, 0, 0);
  }

  /** Result of scoring one amount against the baseline and folding it in. */
  public record Scored(MerchantStats stats, double zscore) {}

  public Scored observe(long amountMinor) {
    double x = Math.log1p(amountMinor / 100.0);
    double z = zscore(x);
    // Do not let obvious outliers poison the baseline they are judged against.
    boolean outlier = count >= MIN_SAMPLES && Math.abs(z) > BASELINE_Z_LIMIT;
    return new Scored(outlier ? this : update(x), z);
  }

  private double zscore(double x) {
    if (count < MIN_SAMPLES) {
      return 0.0;
    }
    double std = Math.sqrt(m2 / (count - 1));
    return (x - mean) / Math.max(std, MIN_STD);
  }

  private MerchantStats update(double x) {
    long n = count + 1;
    double delta = x - mean;
    double newMean = mean + delta / n;
    return new MerchantStats(n, newMean, m2 + delta * (x - newMean));
  }
}
