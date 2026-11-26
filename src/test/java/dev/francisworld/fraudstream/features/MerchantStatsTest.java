package dev.francisworld.fraudstream.features;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MerchantStatsTest {

  private static MerchantStats warmedUp() {
    MerchantStats stats = MerchantStats.empty();
    for (int i = 0; i < 40; i++) {
      stats = stats.observe(2_000 + (i % 5) * 100).stats();
    }
    return stats;
  }

  @Test
  void zscoreIsZeroUntilEnoughSamples() {
    MerchantStats stats = MerchantStats.empty();
    for (int i = 0; i < 29; i++) {
      var scored = stats.observe(1_000_000);
      assertThat(scored.zscore()).isZero();
      stats = scored.stats();
    }
    assertThat(stats.count()).isEqualTo(29);
  }

  @Test
  void outliersScoreHighButDoNotPoisonTheBaseline() {
    MerchantStats baseline = warmedUp();
    var outlier = baseline.observe(500_000);
    assertThat(outlier.zscore()).isGreaterThan(4.0);
    assertThat(outlier.stats()).isEqualTo(baseline);
  }

  @Test
  void typicalAmountsScoreNearZeroAndAreLearned() {
    MerchantStats baseline = warmedUp();
    var typical = baseline.observe(2_200);
    assertThat(Math.abs(typical.zscore())).isLessThan(2.0);
    assertThat(typical.stats().count()).isEqualTo(baseline.count() + 1);
  }

  @Test
  void constantAmountsDoNotProduceInfiniteScores() {
    MerchantStats stats = MerchantStats.empty();
    for (int i = 0; i < 40; i++) {
      stats = stats.observe(1_000).stats();
    }
    assertThat(stats.observe(1_200).zscore()).isFinite().isLessThan(4.0);
  }
}
