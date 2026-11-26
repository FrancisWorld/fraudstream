package dev.francisworld.fraudstream.features;

import static dev.francisworld.fraudstream.support.Txs.tx;
import static org.assertj.core.api.Assertions.assertThat;

import dev.francisworld.fraudstream.model.Transaction;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class AccountFeatureTrackerTest {

  private final AccountFeatureTracker tracker =
      new AccountFeatureTracker(Duration.ofMinutes(5).toMillis());

  private AccountFeatureTracker.Observation observe(AccountState state, Transaction tx) {
    return tracker.observe(state, tx);
  }

  @Test
  void windowsAreInclusiveOfTheCurrentEventAndExactAtTheBoundary() {
    AccountState state = AccountState.empty();
    state = observe(state, tx("a").atSecond(0).amount(10).build()).state();
    state = observe(state, tx("b").atSecond(60).amount(10).build()).state();
    var third = observe(state, tx("c").atSecond(120).amount(10).build());
    // 1m window is [t-60s, t]: b and c
    assertThat(third.features())
        .containsEntry(Features.VELOCITY_COUNT_1M, 2.0)
        .containsEntry(Features.VELOCITY_COUNT_5M, 3.0)
        .containsEntry(Features.VELOCITY_COUNT_1H, 3.0)
        .containsEntry(Features.VELOCITY_SUM_5M, 30.0);
  }

  @Test
  void duplicatesAreDetectedAndLeaveStateUntouched() {
    var first = observe(AccountState.empty(), tx("a").build());
    var again = observe(first.state(), tx("a").build());
    assertThat(again.duplicate()).isTrue();
    assertThat(again.state()).isSameAs(first.state());
  }

  @Test
  void entriesOlderThanAnHourPlusGraceArePruned() {
    AccountState state = AccountState.empty();
    state = observe(state, tx("old").atMinute(0).build()).state();
    state = observe(state, tx("new").atMinute(66).build()).state();
    assertThat(state.entries()).extracting(AccountState.Entry::id).containsExactly("new");
  }

  @Test
  void stateIsBoundedByMaxEntries() {
    AccountState state = AccountState.empty();
    for (int i = 0; i < AccountFeatureTracker.MAX_ENTRIES + 50; i++) {
      state = observe(state, tx("t" + i).atSecond(i).build()).state();
    }
    assertThat(state.entries()).hasSize(AccountFeatureTracker.MAX_ENTRIES);
    assertThat(state.entries().get(0).id()).isEqualTo("t50");
  }

  @Test
  void speedUsesTheWorseOfThePreviousAndNextLocatedNeighbour() {
    AccountState state = AccountState.empty();
    state = observe(state, tx("home").atMinute(0).at(-23.55, -46.63).build()).state();
    state = observe(state, tx("far").atMinute(60).at(40.71, -74.0).build()).state();
    // arrives out of order, between the two: close to home, far from the later one
    var middle = observe(state, tx("mid").atMinute(59).at(-23.56, -46.64).build());
    assertThat(middle.features().get(Features.TRAVEL_SPEED_KMH)).isGreaterThan(100_000.0);
    assertThat(middle.features().get(Features.TRAVEL_DISTANCE_KM)).isGreaterThan(7_000.0);
  }

  @Test
  void eventsWithoutLocationHaveZeroTravelAndAreSkippedAsNeighbours() {
    AccountState state = AccountState.empty();
    state = observe(state, tx("a").atMinute(0).at(-23.55, -46.63).build()).state();
    state = observe(state, tx("nogps").atMinute(1).build()).state();
    var next = observe(state, tx("b").atMinute(2).at(-23.55, -46.63).build());
    assertThat(next.features().get(Features.TRAVEL_SPEED_KMH)).isZero();
    var plain = observe(AccountState.empty(), tx("c").build());
    assertThat(plain.features()).containsEntry(Features.TRAVEL_SPEED_KMH, 0.0);
  }

  @Test
  void simultaneousEventsUseTheMinimumIntervalInsteadOfDividingByZero() {
    AccountState state = observe(AccountState.empty(), tx("a").at(-23.55, -46.63).build()).state();
    var other = observe(state, tx("b").at(-22.90, -43.17).build());
    assertThat(other.features().get(Features.TRAVEL_SPEED_KMH)).isFinite().isGreaterThan(900.0);
  }

  @Test
  void featuresAreExactOverWhatHasArrivedAndEarlierScoresAreNotRevised() {
    var older = tx("a").atSecond(0).build();
    var newer = tx("b").atSecond(30).build();

    // newer first: scored with only itself in the window, then the older one arrives
    var first = observe(AccountState.empty(), newer);
    assertThat(first.features().get(Features.VELOCITY_COUNT_1M)).isEqualTo(1.0);
    var second = observe(first.state(), older);
    // the late arrival's own features are exact: its window ends at its own time
    assertThat(second.features().get(Features.VELOCITY_COUNT_1M)).isEqualTo(1.0);

    // in order, the newer event would have seen the older one
    var inOrder = observe(observe(AccountState.empty(), older).state(), newer);
    assertThat(inOrder.features().get(Features.VELOCITY_COUNT_1M)).isEqualTo(2.0);

    // the stored state is identical either way
    assertThat(second.state()).isEqualTo(inOrder.state());
  }
}
