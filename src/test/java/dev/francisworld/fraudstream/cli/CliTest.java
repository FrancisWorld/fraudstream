package dev.francisworld.fraudstream.cli;

import static dev.francisworld.fraudstream.support.TestApi.INGEST_TOKEN;
import static org.assertj.core.api.Assertions.assertThat;

import dev.francisworld.fraudstream.support.TestApi;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class CliTest {

  private static String run(String... args) {
    PrintStream original = System.out;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    System.setOut(new PrintStream(captured));
    try {
      int code = new CommandLine(new Cli()).execute(args);
      assertThat(code).isZero();
    } finally {
      System.setOut(original);
    }
    return captured.toString();
  }

  @Test
  void generateThenBacktestWritesTextAndJsonReports(@TempDir Path dir) throws IOException {
    Path data = dir.resolve("d.jsonl");
    String generated =
        run("generate", "--accounts", "60", "--days", "5", "--seed", "3", "--out", data.toString());
    assertThat(generated).contains("wrote").contains("fraudulent");
    Path json = dir.resolve("report.json");
    String report = run("backtest", "--data", data.toString(), "--json", json.toString());
    assertThat(report).contains("SYNTHETIC DATA").contains("precision");
    assertThat(Files.readString(json)).contains("\"transactionLevel\"").contains("\"disclaimer\"");
  }

  @Test
  void backtestAcceptsACustomRuleFile(@TempDir Path dir) throws IOException {
    Path data = dir.resolve("d.jsonl");
    run("generate", "--accounts", "40", "--days", "4", "--out", data.toString());
    Path rules = dir.resolve("rules.json");
    Files.writeString(
        rules,
        "{\"name\":\"default\",\"version\":9,\"rules\":[{\"id\":\"big\",\"when\":\"amount > 500\",\"score\":20}]}");
    assertThat(run("backtest", "--data", data.toString(), "--rules", rules.toString()))
        .contains("rule set version      : 9");
  }

  @Test
  void rootCommandPrintsUsage() {
    assertThat(run())
        .contains("fraudstream-cli")
        .contains("generate")
        .contains("backtest")
        .contains("replay");
  }

  @Test
  void replaySendsEveryTransactionThroughThePublicApi() throws Exception {
    List<Labelled> data =
        new SyntheticGenerator(20, 3, 9, Instant.parse("2026-01-05T00:00:00Z")).generate();
    try (TestApi api = TestApi.standalone()) {
      Replay.Summary summary =
          Replay.run(data, URI.create("http://localhost:" + api.http.port()), INGEST_TOKEN, 25);
      assertThat(summary.sent()).isEqualTo(25);
      assertThat(summary.accepted()).isEqualTo(25);
      assertThat(api.producer.history()).hasSize(25);
      Replay.Summary denied =
          Replay.run(data, URI.create("http://localhost:" + api.http.port()), "wrong", 3);
      assertThat(denied.rejected()).isEqualTo(3);
    }
    Replay.Summary down = Replay.run(data, URI.create("http://localhost:1"), "t", 2);
    assertThat(down.failed()).isEqualTo(2);
  }

  @Test
  void replayJsonUsesTheLatestSchemaAndIso8601Times() {
    String json = Replay.toRequestJson(data().get(0).transaction());
    assertThat(json).contains("\"schemaVersion\":2").contains("\"eventTime\":\"2026-01-05T");
  }

  private static List<Labelled> data() {
    return new SyntheticGenerator(20, 3, 9, Instant.parse("2026-01-05T00:00:00Z")).generate();
  }
}
