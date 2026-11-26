package dev.francisworld.fraudstream.features;

import static dev.francisworld.fraudstream.support.Txs.tx;
import static org.assertj.core.api.Assertions.assertThat;

import dev.francisworld.fraudstream.model.Transaction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

class AccountFeatureTrackerPropertyTest {

  private static final long GRACE_MS = 10 * 60_000L;

  /** Offsets in seconds within a window that fits entirely inside the retention horizon. */
  @Provide
  Arbitrary<List<Integer>> offsets() {
    return Arbitraries.integers()
        .between(0, 3_000)
        .list()
        .ofMinSize(1)
        .ofMaxSize(40)
        .uniqueElements();
  }

  private static Transaction at(int offsetSeconds) {
    return tx("t" + offsetSeconds).atSecond(offsetSeconds).amount(1 + offsetSeconds % 7).build();
  }

  /** The tracker agrees with a brute-force count over every event that arrived before. */
  @Property(tries = 200)
  void velocityMatchesBruteForceOracle(@ForAll("offsets") List<Integer> arrival) {
    AccountFeatureTracker tracker = new AccountFeatureTracker(GRACE_MS);
    AccountState state = AccountState.empty();
    List<Integer> seen = new ArrayList<>();
    for (int offset : arrival) {
      var obs = tracker.observe(state, at(offset));
      state = obs.state();
      seen.add(offset);
      long expected1m = seen.stream().filter(o -> o <= offset && o >= offset - 60).count();
      long expected5m = seen.stream().filter(o -> o <= offset && o >= offset - 300).count();
      assertThat(obs.features().get(Features.VELOCITY_COUNT_1M)).isEqualTo((double) expected1m);
      assertThat(obs.features().get(Features.VELOCITY_COUNT_5M)).isEqualTo((double) expected5m);
    }
  }

  /** Once everything has arrived, the stored state does not depend on arrival order. */
  @Property(tries = 200)
  void finalStateIsIndependentOfArrivalOrder(
      @ForAll("offsets") List<Integer> arrival, @ForAll long seed) {
    AccountFeatureTracker tracker = new AccountFeatureTracker(GRACE_MS);
    List<Integer> shuffled = new ArrayList<>(arrival);
    Collections.shuffle(shuffled, new Random(seed));
    assertThat(run(tracker, arrival)).isEqualTo(run(tracker, shuffled));
  }

  private static AccountState run(AccountFeatureTracker tracker, List<Integer> order) {
    AccountState state = AccountState.empty();
    for (int offset : order) {
      state = tracker.observe(state, at(offset)).state();
    }
    return state;
  }

  /** Replaying any event never changes state: ingestion is idempotent. */
  @Property(tries = 200)
  void replayingEventsIsIdempotent(@ForAll("offsets") List<Integer> arrival) {
    AccountFeatureTracker tracker = new AccountFeatureTracker(GRACE_MS);
    AccountState once = run(tracker, arrival);
    AccountState twice = once;
    for (int offset : arrival) {
      var obs = tracker.observe(twice, at(offset));
      assertThat(obs.duplicate()).isTrue();
      twice = obs.state();
    }
    assertThat(twice).isEqualTo(once);
  }

  /** Windows are monotone: a wider window never counts fewer events. */
  @Property(tries = 200)
  void widerWindowsNeverCountLess(@ForAll("offsets") List<Integer> arrival) {
    AccountFeatureTracker tracker = new AccountFeatureTracker(GRACE_MS);
    AccountState state = AccountState.empty();
    for (int offset : arrival) {
      var obs = tracker.observe(state, at(offset));
      state = obs.state();
      var f = obs.features();
      assertThat(f.get(Features.VELOCITY_COUNT_1M))
          .isLessThanOrEqualTo(f.get(Features.VELOCITY_COUNT_5M));
      assertThat(f.get(Features.VELOCITY_COUNT_5M))
          .isLessThanOrEqualTo(f.get(Features.VELOCITY_COUNT_1H));
      assertThat(f.get(Features.VELOCITY_SUM_5M))
          .isLessThanOrEqualTo(f.get(Features.VELOCITY_SUM_1H));
    }
  }
}
