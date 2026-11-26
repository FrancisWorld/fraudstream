package dev.francisworld.fraudstream.features;

import dev.francisworld.fraudstream.model.Transaction;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Pure per-account feature logic: exact event-time velocity windows, duplicate detection and
 * impossible-travel speed. It is deliberately free of Kafka types so it can be tested and fuzzed
 * directly.
 *
 * <p>Guarantees, precisely:
 *
 * <ul>
 *   <li>The stored state after a set of events is independent of the order they arrived in.
 *   <li>The features of an event are exact over the events ingested <em>before it</em>, evaluated
 *       in a window ending at the event's own timestamp. An event that arrives out of order
 *       therefore sees its older neighbours correctly and its newer neighbours that already
 *       arrived.
 *   <li>Features are computed once, on arrival. A later out-of-order arrival can change what the
 *       features of an already scored event would have been, but that event is not re-scored and an
 *       alert already emitted is not retracted. Scoring after the grace period closes would make
 *       every score order-independent at the price of delaying every alert by the grace.
 * </ul>
 */
public final class AccountFeatureTracker {

  static final long MINUTE_MS = 60_000L;
  static final long HOUR_MS = 60 * MINUTE_MS;
  static final int MAX_ENTRIES = 2_000;
  private static final long MIN_TRAVEL_INTERVAL_MS = 1_000L;
  private static final Comparator<AccountState.Entry> ORDER =
      Comparator.comparingLong(AccountState.Entry::ts).thenComparing(AccountState.Entry::id);

  private final long graceMs;

  public AccountFeatureTracker(long graceMs) {
    this.graceMs = graceMs;
  }

  /** Result of observing one transaction. */
  public record Observation(AccountState state, boolean duplicate, Map<String, Double> features) {}

  public Observation observe(AccountState previous, Transaction tx) {
    for (AccountState.Entry e : previous.entries()) {
      if (e.id().equals(tx.transactionId())) {
        return new Observation(previous, true, Map.of());
      }
    }
    AccountState.Entry entry =
        new AccountState.Entry(
            tx.transactionId(), tx.eventTimeMs(), tx.amountMinor(), tx.latitude(), tx.longitude());
    List<AccountState.Entry> entries = insertSorted(previous.entries(), entry);
    entries = prune(entries);
    Map<String, Double> features = new TreeMap<>();
    velocity(entries, tx.eventTimeMs(), features);
    travel(entries, entry, features);
    return new Observation(new AccountState(entries), false, features);
  }

  private static List<AccountState.Entry> insertSorted(
      List<AccountState.Entry> sorted, AccountState.Entry entry) {
    List<AccountState.Entry> out = new ArrayList<>(sorted.size() + 1);
    boolean placed = false;
    for (AccountState.Entry e : sorted) {
      if (!placed && ORDER.compare(entry, e) < 0) {
        out.add(entry);
        placed = true;
      }
      out.add(e);
    }
    if (!placed) {
      out.add(entry);
    }
    return out;
  }

  private List<AccountState.Entry> prune(List<AccountState.Entry> entries) {
    long maxTs = entries.get(entries.size() - 1).ts();
    long floor = maxTs - HOUR_MS - graceMs;
    int from = 0;
    while (from < entries.size() && entries.get(from).ts() < floor) {
      from++;
    }
    from = Math.max(from, entries.size() - MAX_ENTRIES);
    return from == 0 ? entries : new ArrayList<>(entries.subList(from, entries.size()));
  }

  private static void velocity(
      List<AccountState.Entry> entries, long t, Map<String, Double> features) {
    long[] counts = new long[3];
    long[] sums = new long[3];
    long[] widths = {MINUTE_MS, 5 * MINUTE_MS, HOUR_MS};
    for (AccountState.Entry e : entries) {
      if (e.ts() > t) {
        continue;
      }
      for (int i = 0; i < widths.length; i++) {
        if (e.ts() >= t - widths[i]) {
          counts[i]++;
          sums[i] += e.amountMinor();
        }
      }
    }
    features.put(Features.VELOCITY_COUNT_1M, (double) counts[0]);
    features.put(Features.VELOCITY_COUNT_5M, (double) counts[1]);
    features.put(Features.VELOCITY_COUNT_1H, (double) counts[2]);
    features.put(Features.VELOCITY_SUM_5M, sums[1] / 100.0);
    features.put(Features.VELOCITY_SUM_1H, sums[2] / 100.0);
  }

  private static void travel(
      List<AccountState.Entry> entries, AccountState.Entry current, Map<String, Double> features) {
    double bestSpeed = 0;
    double bestDistance = 0;
    if (current.located()) {
      int at = entries.indexOf(current);
      AccountState.Entry before = null;
      for (int i = at - 1; i >= 0 && before == null; i--) {
        if (entries.get(i).located()) {
          before = entries.get(i);
        }
      }
      AccountState.Entry after = null;
      for (int i = at + 1; i < entries.size() && after == null; i++) {
        if (entries.get(i).located()) {
          after = entries.get(i);
        }
      }
      for (AccountState.Entry neighbour : new AccountState.Entry[] {before, after}) {
        if (neighbour == null) {
          continue;
        }
        double km =
            GeoMath.distanceKm(current.lat(), current.lon(), neighbour.lat(), neighbour.lon());
        long dt = Math.max(Math.abs(current.ts() - neighbour.ts()), MIN_TRAVEL_INTERVAL_MS);
        double speed = km / (dt / 3_600_000.0);
        if (speed > bestSpeed) {
          bestSpeed = speed;
          bestDistance = km;
        }
      }
    }
    features.put(Features.TRAVEL_SPEED_KMH, bestSpeed);
    features.put(Features.TRAVEL_DISTANCE_KM, bestDistance);
  }
}
