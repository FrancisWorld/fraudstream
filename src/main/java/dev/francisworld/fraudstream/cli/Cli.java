package dev.francisworld.fraudstream.cli;

import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.serde.Json;
import dev.francisworld.fraudstream.topology.FraudSettings;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** {@code fraudstream-cli}: generate synthetic data, backtest rules, replay into a live API. */
@Command(
    name = "fraudstream-cli",
    mixinStandardHelpOptions = true,
    subcommands = {Cli.Generate.class, Cli.BacktestCommand.class, Cli.ReplayCommand.class},
    description = "Synthetic data, rule backtesting and replay for fraudstream")
public final class Cli implements Runnable {

  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new Cli()).execute(args));
  }

  static RuleSetDocument loadRules(Path file) throws IOException {
    if (file != null) {
      return Json.MAPPER.readValue(Files.readAllBytes(file), RuleSetDocument.class);
    }
    try (InputStream in = Cli.class.getResourceAsStream("/rules/default.json")) {
      return Json.MAPPER.readValue(in, RuleSetDocument.class);
    }
  }

  /** {@code generate}: write a labelled synthetic dataset. */
  @Command(
      name = "generate",
      mixinStandardHelpOptions = true,
      description = "Generate a labelled synthetic dataset (JSON Lines)")
  static final class Generate implements Callable<Integer> {
    @Option(names = "--accounts", defaultValue = "300")
    int accounts;

    @Option(names = "--days", defaultValue = "10")
    int days;

    @Option(names = "--seed", defaultValue = "42")
    long seed;

    @Option(
        names = "--start",
        defaultValue = "2026-01-05T00:00:00Z",
        description = "First day, ISO instant")
    Instant start;

    @Option(names = "--out", required = true)
    Path out;

    @Override
    public Integer call() throws IOException {
      List<Labelled> data = new SyntheticGenerator(accounts, days, seed, start).generate();
      Dataset.write(out, data);
      long fraud = data.stream().filter(l -> l.label().fraud()).count();
      System.out.printf("wrote %d transactions (%d fraudulent) to %s%n", data.size(), fraud, out);
      return 0;
    }
  }

  /** {@code backtest}: score a rule set against a labelled dataset. */
  @Command(
      name = "backtest",
      mixinStandardHelpOptions = true,
      description = "Replay a dataset through the topology and report precision and recall")
  static final class BacktestCommand implements Callable<Integer> {
    @Option(names = "--data", required = true)
    Path data;

    @Option(names = "--rules", description = "Rule set JSON; defaults to the bundled one")
    Path rules;

    @Option(names = "--grace", defaultValue = "PT5M")
    Duration grace;

    @Option(
        names = "--max-disorder-seconds",
        defaultValue = "0",
        description = "Delay arrivals randomly by up to N seconds")
    int disorder;

    @Option(names = "--seed", defaultValue = "7")
    long seed;

    @Option(names = "--json", description = "Also write the report as JSON")
    Path json;

    @Override
    public Integer call() throws IOException {
      FraudSettings settings = new FraudSettings(grace, Duration.ofHours(24), "default");
      RuleSetDocument ruleSet = loadRules(rules);
      BacktestReport report =
          Backtest.run(
              Dataset.read(data),
              ruleSet,
              new FraudSettings(grace, settings.deviceWindow(), ruleSet.name()),
              disorder,
              seed);
      System.out.print(report.toText());
      if (json != null) {
        Files.writeString(
            json, Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report));
      }
      return 0;
    }
  }

  /** {@code replay}: push a dataset into a running service. */
  @Command(
      name = "replay",
      mixinStandardHelpOptions = true,
      description = "Send a dataset to a running API")
  static final class ReplayCommand implements Callable<Integer> {
    @Option(names = "--data", required = true)
    Path data;

    @Option(names = "--api-url", defaultValue = "http://localhost:8080")
    URI apiUrl;

    @Option(
        names = "--token-env",
        defaultValue = "FRAUDSTREAM_INGEST_TOKEN",
        description = "Environment variable holding the ingest token")
    String tokenEnv;

    @Option(
        names = "--limit",
        defaultValue = "0",
        description = "Stop after N transactions (0 = all)")
    int limit;

    @Override
    public Integer call() throws IOException, InterruptedException {
      String token = System.getenv(tokenEnv);
      if (token == null || token.isBlank()) {
        System.err.println("Environment variable " + tokenEnv + " is not set");
        return 2;
      }
      Replay.Summary s = Replay.run(Dataset.read(data), apiUrl, token, limit);
      System.out.printf(
          "sent=%d accepted=%d rejected=%d failed=%d%n",
          s.sent(), s.accepted(), s.rejected(), s.failed());
      return s.failed() == 0 && s.rejected() == 0 ? 0 : 1;
    }
  }
}
