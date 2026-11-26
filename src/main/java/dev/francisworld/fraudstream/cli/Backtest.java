package dev.francisworld.fraudstream.cli;

import dev.francisworld.fraudstream.model.Alert;
import dev.francisworld.fraudstream.model.DeadLetter;
import dev.francisworld.fraudstream.model.LateEvent;
import dev.francisworld.fraudstream.model.RuleHit;
import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.model.Transaction;
import dev.francisworld.fraudstream.rules.RuleCompiler;
import dev.francisworld.fraudstream.schema.SchemaRegistry;
import dev.francisworld.fraudstream.schema.TransactionCodec;
import dev.francisworld.fraudstream.serde.Json;
import dev.francisworld.fraudstream.serde.JsonSerde;
import dev.francisworld.fraudstream.topology.FraudMetrics;
import dev.francisworld.fraudstream.topology.FraudSettings;
import dev.francisworld.fraudstream.topology.FraudTopology;
import dev.francisworld.fraudstream.topology.Topics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

/**
 * Replays a labelled dataset through the production topology, in process and without a broker, and
 * scores the alerts against the ground truth.
 */
public final class Backtest {

  private Backtest() {}

  /**
   * @param maxDisorderSeconds each event's arrival is delayed by a random 0..N seconds relative to
   *     its event time, so values above the grace period produce late events
   */
  public static BacktestReport run(
      List<Labelled> data,
      RuleSetDocument rules,
      FraudSettings settings,
      int maxDisorderSeconds,
      long seed)
      throws IOException {
    RuleCompiler.compile(rules);
    Path stateDir = Files.createTempDirectory("fraudstream-backtest");
    Topics topics = Topics.defaults();
    TransactionCodec codec = new TransactionCodec(SchemaRegistry.load());
    var topology =
        new FraudTopology(topics, settings, new FraudMetrics(new SimpleMeterRegistry()), codec)
            .build();
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "backtest-" + UUID.randomUUID());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "backtest:9092");
    props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    long began = System.nanoTime();
    Set<String> alerted = new HashSet<>();
    Map<String, List<RuleHit>> hitsByTransaction = new HashMap<>();
    int late = 0;
    int dead = 0;
    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props, Instant.EPOCH)) {
      var strings = new StringSerializer();
      var in = driver.createInputTopic(topics.transactions(), strings, new ByteArraySerializer());
      var rulesIn = driver.createInputTopic(topics.rules(), strings, new ByteArraySerializer());
      TestOutputTopic<String, Alert> alertsOut =
          driver.createOutputTopic(
              topics.alerts(),
              Serdes.String().deserializer(),
              JsonSerde.of(Alert.class).deserializer());
      TestOutputTopic<String, LateEvent> lateOut =
          driver.createOutputTopic(
              topics.lateEvents(),
              Serdes.String().deserializer(),
              JsonSerde.of(LateEvent.class).deserializer());
      TestOutputTopic<String, DeadLetter> deadOut =
          driver.createOutputTopic(
              topics.deadLetters(),
              Serdes.String().deserializer(),
              JsonSerde.of(DeadLetter.class).deserializer());
      rulesIn.pipeInput(rules.name(), Json.write(rules));
      for (Transaction tx : arrivalOrder(data, maxDisorderSeconds, seed)) {
        in.pipeInput(
            tx.accountId(),
            codec.encode(tx, SchemaRegistry.V2),
            Instant.ofEpochMilli(tx.eventTimeMs()));
        for (Alert alert : alertsOut.readValuesToList()) {
          alerted.add(alert.transactionId());
          hitsByTransaction.put(alert.transactionId(), alert.hits());
        }
        late += lateOut.readValuesToList().size();
        dead += deadOut.readValuesToList().size();
      }
    } finally {
      deleteRecursively(stateDir);
    }
    double elapsed = (System.nanoTime() - began) / 1e9;
    return score(data, rules.version(), alerted, hitsByTransaction, late, dead, elapsed);
  }

  static List<Transaction> arrivalOrder(List<Labelled> data, int maxDisorderSeconds, long seed) {
    Random random = new Random(seed);
    record Arrival(Transaction tx, long arrivalMs) {}
    List<Arrival> arrivals = new ArrayList<>(data.size());
    for (Labelled l : data) {
      long delay =
          maxDisorderSeconds <= 0 ? 0 : (long) (random.nextDouble() * maxDisorderSeconds * 1000);
      arrivals.add(new Arrival(l.transaction(), l.transaction().eventTimeMs() + delay));
    }
    arrivals.sort(Comparator.comparingLong(Arrival::arrivalMs));
    return arrivals.stream().map(Arrival::tx).toList();
  }

  static BacktestReport score(
      List<Labelled> data,
      long ruleSetVersion,
      Set<String> alerted,
      Map<String, List<RuleHit>> hits,
      int late,
      int dead,
      double elapsedSeconds) {
    int tp = 0;
    int fp = 0;
    int fn = 0;
    int tn = 0;
    Map<String, int[]> pattern = new TreeMap<>(); // fraudTxns, flaggedTxns
    Map<String, Set<String>> episodesByPattern = new TreeMap<>();
    Map<String, Set<String>> detectedByPattern = new TreeMap<>();
    Map<String, int[]> rule = new TreeMap<>(); // hits, tp
    Set<String> allEpisodes = new HashSet<>();
    Set<String> detectedEpisodes = new HashSet<>();
    for (Labelled l : data) {
      String id = l.transaction().transactionId();
      boolean flagged = alerted.contains(id);
      boolean fraud = l.label().fraud();
      if (fraud && flagged) {
        tp++;
      } else if (fraud) {
        fn++;
      } else if (flagged) {
        fp++;
      } else {
        tn++;
      }
      if (fraud) {
        String p = l.label().pattern();
        pattern.computeIfAbsent(p, k -> new int[2])[0]++;
        episodesByPattern.computeIfAbsent(p, k -> new HashSet<>()).add(l.label().episode());
        allEpisodes.add(l.label().episode());
        if (flagged) {
          pattern.get(p)[1]++;
          detectedByPattern.computeIfAbsent(p, k -> new HashSet<>()).add(l.label().episode());
          detectedEpisodes.add(l.label().episode());
        }
      }
      if (flagged) {
        for (RuleHit hit : hits.getOrDefault(id, List.of())) {
          int[] counts = rule.computeIfAbsent(hit.ruleId(), k -> new int[2]);
          counts[0]++;
          if (fraud) {
            counts[1]++;
          }
        }
      }
    }
    Map<String, BacktestReport.PatternStats> byPattern = new TreeMap<>();
    pattern.forEach((p, c) ->
        byPattern.put(
            p,
            new BacktestReport.PatternStats(
                episodesByPattern.get(p).size(),
                detectedByPattern.getOrDefault(p, Set.of()).size(),
                c[0],
                c[1])));
    Map<String, BacktestReport.RuleStats> byRule = new TreeMap<>();
    rule.forEach((r, c) -> byRule.put(r, new BacktestReport.RuleStats(c[0], c[1])));
    int legit = fp + tn;
    return new BacktestReport(
        BacktestReport.DISCLAIMER,
        ruleSetVersion,
        data.size(),
        tp + fn,
        allEpisodes.size(),
        alerted.size(),
        late,
        dead,
        new BacktestReport.Confusion(tp, fp, fn, tn),
        allEpisodes.isEmpty() ? 0 : (double) detectedEpisodes.size() / allEpisodes.size(),
        legit == 0 ? 0 : 1000.0 * fp / legit,
        byPattern,
        byRule,
        elapsedSeconds,
        elapsedSeconds == 0 ? 0 : data.size() / elapsedSeconds);
  }

  private static void deleteRecursively(Path dir) throws IOException {
    try (Stream<Path> files = Files.walk(dir)) {
      files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    }
  }
}
