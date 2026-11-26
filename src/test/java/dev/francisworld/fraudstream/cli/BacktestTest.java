package dev.francisworld.fraudstream.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.francisworld.fraudstream.model.RuleDefinition;
import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.rules.RuleSetInvalidException;
import dev.francisworld.fraudstream.support.PipelineFixture;
import dev.francisworld.fraudstream.topology.FraudSettings;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BacktestTest {

  private static final Instant START = Instant.parse("2026-01-05T00:00:00Z");
  private static List<Labelled> data;

  @BeforeAll
  static void generate() {
    data = new SyntheticGenerator(150, 8, 42, START).generate();
  }

  @Test
  void generatorIsDeterministicSortedAndLabelled() {
    List<Labelled> again = new SyntheticGenerator(150, 8, 42, START).generate();
    assertThat(again).isEqualTo(data);
    assertThat(new SyntheticGenerator(150, 8, 43, START).generate()).isNotEqualTo(data);
    assertThat(data).extracting(l -> l.transaction().eventTimeMs()).isSorted();
    assertThat(data).extracting(l -> l.transaction().transactionId()).doesNotHaveDuplicates();
    long fraud = data.stream().filter(l -> l.label().fraud()).count();
    assertThat(fraud).isBetween(data.size() / 200L, data.size() / 10L);
    assertThat(
            data.stream().filter(l -> l.label().fraud()).map(l -> l.label().pattern()).distinct())
        .contains(
            SyntheticGenerator.VELOCITY,
            SyntheticGenerator.TRAVEL,
            SyntheticGenerator.DEVICE_RING,
            SyntheticGenerator.LOW_AND_SLOW);
  }

  @Test
  void generatorRejectsDegenerateSizes() {
    assertThatThrownBy(() -> new SyntheticGenerator(5, 10, 1, START))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new SyntheticGenerator(100, 1, 1, START))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void datasetsRoundTripThroughJsonLines(@TempDir Path dir) throws IOException {
    Path file = dir.resolve("data.jsonl");
    Dataset.write(file, data);
    assertThat(Dataset.read(file)).isEqualTo(data);
    Files.writeString(file, "{\"transaction\":{}}\n");
    assertThatThrownBy(() -> Dataset.read(file))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Line 1");
  }

  @Test
  void shippedRulesCatchMostEpisodesButNotEverythingAndAreNotPerfect() throws IOException {
    BacktestReport report =
        Backtest.run(data, PipelineFixture.defaultRules(1), FraudSettings.defaults(), 0, 1);
    assertThat(report.transactions()).isEqualTo(data.size());
    assertThat(report.deadLetters()).isZero();
    assertThat(report.lateEvents()).isZero();
    assertThat(report.episodeRecall()).isBetween(0.5, 0.99);
    assertThat(report.transactionLevel().precision()).isBetween(0.3, 0.99);
    assertThat(report.byPattern().get(SyntheticGenerator.LOW_AND_SLOW).episodesDetected()).isZero();
    assertThat(report.byPattern().get(SyntheticGenerator.TRAVEL).episodesDetected()).isPositive();
    assertThat(report.byPattern().get(SyntheticGenerator.VELOCITY).episodesDetected()).isPositive();
    assertThat(report.byRule()).containsKeys("velocity-burst", "impossible-travel");
    assertThat(report.toText())
        .contains("SYNTHETIC DATA")
        .contains("per rule")
        .contains("recall (episode)");
    BacktestReport.Confusion c = report.transactionLevel();
    assertThat(c.tp() + c.fp() + c.fn() + c.tn()).isEqualTo(data.size());
  }

  @Test
  void backtestsAreReproducible() throws IOException {
    var a = Backtest.run(data, PipelineFixture.defaultRules(1), FraudSettings.defaults(), 60, 5);
    var b = Backtest.run(data, PipelineFixture.defaultRules(1), FraudSettings.defaults(), 60, 5);
    assertThat(a.transactionLevel()).isEqualTo(b.transactionLevel());
    assertThat(a.byRule()).isEqualTo(b.byRule());
  }

  @Test
  void disorderBeyondGraceProducesLateEventsAndLowersRecall() throws IOException {
    var ordered =
        Backtest.run(data, PipelineFixture.defaultRules(1), FraudSettings.defaults(), 0, 1);
    var chaotic =
        Backtest.run(
            data,
            PipelineFixture.defaultRules(1),
            new FraudSettings(Duration.ofMinutes(1), Duration.ofHours(24), "default"),
            1_800,
            1);
    assertThat(chaotic.lateEvents()).isPositive();
    assertThat(chaotic.transactionLevel().recall()).isLessThan(ordered.transactionLevel().recall());
  }

  @Test
  void aStricterRuleSetTradesRecallForPrecision() throws IOException {
    RuleSetDocument strict =
        new RuleSetDocument(
            "default",
            2,
            List.of(
                new RuleDefinition(
                    "travel-only", "", "travel_speed_kmh > 900 and travel_distance_km > 100", 80)));
    var report = Backtest.run(data, strict, FraudSettings.defaults(), 0, 1);
    var baseline =
        Backtest.run(data, PipelineFixture.defaultRules(1), FraudSettings.defaults(), 0, 1);
    assertThat(report.ruleSetVersion()).isEqualTo(2);
    assertThat(report.transactionLevel().recall()).isLessThan(baseline.transactionLevel().recall());
    assertThat(report.transactionLevel().precision())
        .isGreaterThan(baseline.transactionLevel().precision());
  }

  @Test
  void invalidRuleSetsAreRejectedBeforeReplay() {
    RuleSetDocument bad =
        new RuleSetDocument("default", 1, List.of(new RuleDefinition("x", "", "nope > 1", 5)));
    assertThatThrownBy(() -> Backtest.run(data, bad, FraudSettings.defaults(), 0, 1))
        .isInstanceOf(RuleSetInvalidException.class);
  }

  @Test
  void scoringArithmeticIsExact() {
    List<Labelled> tiny =
        List.of(
            labelled("a", true, "p", "e1"),
            labelled("b", true, "p", "e1"),
            labelled("c", true, "q", "e2"),
            labelled("d", false, null, null),
            labelled("e", false, null, null));
    var report =
        Backtest.score(
            tiny,
            1,
            Set.of("a", "d"),
            Map.of(
                "a",
                List.of(new dev.francisworld.fraudstream.model.RuleHit("r1", 10)),
                "d",
                List.of(new dev.francisworld.fraudstream.model.RuleHit("r1", 10))),
            0,
            0,
            2.0);
    assertThat(report.transactionLevel()).isEqualTo(new BacktestReport.Confusion(1, 1, 2, 1));
    assertThat(report.transactionLevel().precision()).isEqualTo(0.5);
    assertThat(report.transactionLevel().recall())
        .isCloseTo(1.0 / 3, org.assertj.core.data.Offset.offset(1e-9));
    assertThat(report.episodeRecall()).isEqualTo(0.5);
    assertThat(report.falseAlertsPer1000Legitimate()).isEqualTo(500.0);
    assertThat(report.byRule().get("r1")).isEqualTo(new BacktestReport.RuleStats(2, 1));
    assertThat(report.byPattern().get("p")).isEqualTo(new BacktestReport.PatternStats(1, 1, 2, 1));
    assertThat(report.throughputPerSecond()).isEqualTo(2.5);
  }

  @Test
  void emptyInputsDoNotDivideByZero() {
    var report = Backtest.score(List.of(), 1, Set.of(), Map.of(), 0, 0, 0);
    assertThat(report.transactionLevel().precision()).isZero();
    assertThat(report.transactionLevel().f1()).isZero();
    assertThat(report.episodeRecall()).isZero();
    assertThat(report.throughputPerSecond()).isZero();
    assertThat(new BacktestReport.RuleStats(0, 0).precision()).isZero();
  }

  private static Labelled labelled(String id, boolean fraud, String pattern, String episode) {
    return new Labelled(
        dev.francisworld.fraudstream.support.Txs.tx(id).build(),
        new Labelled.Label(fraud, pattern, episode));
  }
}
