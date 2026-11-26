package dev.francisworld.fraudstream.support;

import dev.francisworld.fraudstream.cases.AccountCases;
import dev.francisworld.fraudstream.cases.CaseCommand;
import dev.francisworld.fraudstream.cases.CaseEvent;
import dev.francisworld.fraudstream.cases.CommandType;
import dev.francisworld.fraudstream.model.AccountRisk;
import dev.francisworld.fraudstream.model.Alert;
import dev.francisworld.fraudstream.model.DeadLetter;
import dev.francisworld.fraudstream.model.LateEvent;
import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.model.Transaction;
import dev.francisworld.fraudstream.schema.SchemaRegistry;
import dev.francisworld.fraudstream.schema.TransactionCodec;
import dev.francisworld.fraudstream.serde.Json;
import dev.francisworld.fraudstream.serde.JsonSerde;
import dev.francisworld.fraudstream.topology.FraudMetrics;
import dev.francisworld.fraudstream.topology.FraudSettings;
import dev.francisworld.fraudstream.topology.FraudTopology;
import dev.francisworld.fraudstream.topology.StoreNames;
import dev.francisworld.fraudstream.topology.Topics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;

/** The full topology wired to a TopologyTestDriver, with typed helpers; no broker involved. */
public final class PipelineFixture implements AutoCloseable {

  public final SimpleMeterRegistry meters = new SimpleMeterRegistry();
  public final Topics topics = Topics.defaults();
  public final SchemaRegistry registry = SchemaRegistry.load();
  public final TransactionCodec codec = new TransactionCodec(registry);

  private final Path stateDir;
  private final TopologyTestDriver driver;
  private final TestInputTopic<String, byte[]> transactionsIn;
  private final TestInputTopic<String, byte[]> rulesIn;
  private final TestInputTopic<String, byte[]> commandsIn;
  private final TestOutputTopic<String, Alert> alertsOut;
  private final TestOutputTopic<String, DeadLetter> dlqOut;
  private final TestOutputTopic<String, LateEvent> lateOut;
  private final TestOutputTopic<String, CaseEvent> caseEventsOut;

  public PipelineFixture() {
    this(FraudSettings.defaults());
  }

  public PipelineFixture(FraudSettings settings) {
    try {
      stateDir = Files.createTempDirectory("fraudstream-test");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraudstream-test-" + UUID.randomUUID());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    var topology = new FraudTopology(topics, settings, new FraudMetrics(meters), codec).build();
    driver = new TopologyTestDriver(topology, props, T0_WALL);
    var strings = new StringSerializer();
    transactionsIn =
        driver.createInputTopic(topics.transactions(), strings, new ByteArraySerializer());
    rulesIn = driver.createInputTopic(topics.rules(), strings, new ByteArraySerializer());
    commandsIn = driver.createInputTopic(topics.caseCommands(), strings, new ByteArraySerializer());
    var d = Serdes.String().deserializer();
    alertsOut =
        driver.createOutputTopic(topics.alerts(), d, JsonSerde.of(Alert.class).deserializer());
    dlqOut =
        driver.createOutputTopic(
            topics.deadLetters(), d, JsonSerde.of(DeadLetter.class).deserializer());
    lateOut =
        driver.createOutputTopic(
            topics.lateEvents(), d, JsonSerde.of(LateEvent.class).deserializer());
    caseEventsOut =
        driver.createOutputTopic(
            topics.caseEvents(), d, JsonSerde.of(CaseEvent.class).deserializer());
  }

  private static final Instant T0_WALL = Instant.parse("2026-03-02T00:00:00Z");

  public static RuleSetDocument defaultRules(long version) {
    try (InputStream in = PipelineFixture.class.getResourceAsStream("/rules/default.json")) {
      RuleSetDocument doc = Json.MAPPER.readValue(in, RuleSetDocument.class);
      return new RuleSetDocument(doc.name(), version, doc.rules());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void publishRules(RuleSetDocument doc) {
    rulesIn.pipeInput(doc.name(), Json.write(doc));
  }

  public void publishRawRules(byte[] payload) {
    rulesIn.pipeInput("default", payload);
  }

  /** Sends a transaction in the latest schema generation. */
  public void send(Transaction tx) {
    send(tx, registry.latestId());
  }

  public void send(Transaction tx, int schemaId) {
    sendRaw(tx.accountId(), codec.encode(tx, schemaId), Instant.ofEpochMilli(tx.eventTimeMs()));
  }

  public void sendRaw(String key, byte[] payload, Instant timestamp) {
    transactionsIn.pipeInput(key, payload, timestamp);
  }

  public void command(CaseCommand command) {
    commandsIn.pipeInput(
        command.accountId(), Json.write(command), Instant.ofEpochMilli(command.requestedAtMs()));
  }

  public void sendRawCommand(String key, byte[] payload) {
    commandsIn.pipeInput(key, payload);
  }

  /** Commands are stamped one minute after T0 so they never run ahead of the event timeline. */
  public static CaseCommand command(
      String commandId, String caseId, CommandType type, String actor, String comment) {
    String account = AccountCases.accountOf(caseId);
    return new CaseCommand(
        commandId, account, caseId, type, actor, comment, Txs.T0.toEpochMilli() + 60_000L);
  }

  public List<Alert> alerts() {
    return alertsOut.readValuesToList();
  }

  public List<DeadLetter> deadLetters() {
    return dlqOut.readValuesToList();
  }

  public List<LateEvent> lateEvents() {
    return lateOut.readValuesToList();
  }

  public List<CaseEvent> caseEvents() {
    return caseEventsOut.readValuesToList();
  }

  public KeyValueStore<String, AccountCases> casesStore() {
    return driver.getKeyValueStore(StoreNames.CASES);
  }

  public KeyValueStore<String, AccountRisk> riskStore() {
    return driver.getKeyValueStore(StoreNames.ACCOUNT_RISK);
  }

  public KeyValueStore<String, byte[]> rulesStore() {
    return driver.getKeyValueStore(StoreNames.RULES);
  }

  /** Feeds a record into any input topic, as if a producer had written it. */
  public void pipe(String topic, String key, byte[] value, Instant timestamp) {
    driver
        .createInputTopic(topic, new StringSerializer(), new ByteArraySerializer())
        .pipeInput(key, value, timestamp);
  }

  public <V> KeyValueStore<String, V> store(String name) {
    return driver.getKeyValueStore(name);
  }

  public double counter(String name) {
    var counter = meters.find(name).counter();
    return counter == null ? 0 : counter.count();
  }

  public double counter(String name, String tag, String value) {
    var counter = meters.find(name).tag(tag, value).counter();
    return counter == null ? 0 : counter.count();
  }

  public static FraudSettings settings(Duration grace) {
    return new FraudSettings(grace, Duration.ofHours(24), "default");
  }

  @Override
  public void close() {
    driver.close();
    try (Stream<Path> files = Files.walk(stateDir)) {
      files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    } catch (IOException e) {
      // best effort cleanup of a temp directory
    }
  }
}
