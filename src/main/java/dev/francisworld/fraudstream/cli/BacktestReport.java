package dev.francisworld.fraudstream.cli;

import java.util.Locale;
import java.util.Map;

/** Outcome of replaying a labelled dataset through the real topology. */
public record BacktestReport(
    String disclaimer,
    long ruleSetVersion,
    int transactions,
    int fraudTransactions,
    int episodes,
    int alertedTransactions,
    int lateEvents,
    int deadLetters,
    Confusion transactionLevel,
    double episodeRecall,
    double falseAlertsPer1000Legitimate,
    Map<String, PatternStats> byPattern,
    Map<String, RuleStats> byRule,
    double elapsedSeconds,
    double throughputPerSecond) {

  public static final String DISCLAIMER =
      "SYNTHETIC DATA: invented transactions with injected fraud patterns. These figures say how the"
          + " rules behave on this generator, not how they would perform on real fraud.";

  /** Transaction-level confusion matrix. A transaction counts as flagged when any rule matched. */
  public record Confusion(int tp, int fp, int fn, int tn) {
    public double precision() {
      return tp + fp == 0 ? 0 : (double) tp / (tp + fp);
    }

    public double recall() {
      return tp + fn == 0 ? 0 : (double) tp / (tp + fn);
    }

    public double f1() {
      double p = precision();
      double r = recall();
      return p + r == 0 ? 0 : 2 * p * r / (p + r);
    }
  }

  /** Per fraud pattern: how many episodes and transactions were caught. */
  public record PatternStats(
      int episodes, int episodesDetected, int fraudTransactions, int fraudTransactionsFlagged) {}

  /** Per rule: how often it fired and how often it was right. */
  public record RuleStats(int hits, int truePositives) {
    public double precision() {
      return hits == 0 ? 0 : (double) truePositives / hits;
    }
  }

  public String toText() {
    StringBuilder s = new StringBuilder();
    s.append(disclaimer).append("\n\n");
    s.append(String.format(Locale.ROOT, "rule set version      : %d%n", ruleSetVersion));
    s.append(
        String.format(
            Locale.ROOT,
            "transactions          : %d (%d fraudulent, %d episodes)%n",
            transactions,
            fraudTransactions,
            episodes));
    s.append(
        String.format(
            Locale.ROOT,
            "alerted transactions  : %d  (late: %d, dead-lettered: %d)%n",
            alertedTransactions,
            lateEvents,
            deadLetters));
    s.append(
        String.format(Locale.ROOT, "precision             : %.3f%n", transactionLevel.precision()));
    s.append(
        String.format(Locale.ROOT, "recall (transaction)  : %.3f%n", transactionLevel.recall()));
    s.append(String.format(Locale.ROOT, "recall (episode)      : %.3f%n", episodeRecall));
    s.append(String.format(Locale.ROOT, "F1                    : %.3f%n", transactionLevel.f1()));
    s.append(
        String.format(
            Locale.ROOT, "false alerts / 1000 legit: %.2f%n", falseAlertsPer1000Legitimate));
    s.append(
        String.format(
            Locale.ROOT,
            "confusion             : tp=%d fp=%d fn=%d tn=%d%n",
            transactionLevel.tp(),
            transactionLevel.fp(),
            transactionLevel.fn(),
            transactionLevel.tn()));
    s.append("\nper pattern (episodes detected / total, fraud txns flagged / total)\n");
    byPattern.forEach((k, v) ->
        s.append(
            String.format(
                Locale.ROOT,
                "  %-18s %3d / %-3d   %4d / %-4d%n",
                k,
                v.episodesDetected(),
                v.episodes(),
                v.fraudTransactionsFlagged(),
                v.fraudTransactions())));
    s.append("\nper rule (hits, precision)\n");
    byRule.forEach((k, v) ->
        s.append(String.format(Locale.ROOT, "  %-26s %5d   %.3f%n", k, v.hits(), v.precision())));
    s.append(
        String.format(
            Locale.ROOT,
            "%nreplayed in %.1fs (%.0f txn/s)%n",
            elapsedSeconds,
            throughputPerSecond));
    return s.toString();
  }
}
