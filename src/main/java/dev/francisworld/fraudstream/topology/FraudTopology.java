package dev.francisworld.fraudstream.topology;

import dev.francisworld.fraudstream.cases.AccountCases;
import dev.francisworld.fraudstream.cases.CaseCommand;
import dev.francisworld.fraudstream.cases.CaseEvent;
import dev.francisworld.fraudstream.cases.CaseInput;
import dev.francisworld.fraudstream.features.AccountState;
import dev.francisworld.fraudstream.features.MerchantStats;
import dev.francisworld.fraudstream.model.AccountRisk;
import dev.francisworld.fraudstream.model.Alert;
import dev.francisworld.fraudstream.model.DeadLetter;
import dev.francisworld.fraudstream.model.DeviceLinks;
import dev.francisworld.fraudstream.model.EnrichedTransaction;
import dev.francisworld.fraudstream.model.LateEvent;
import dev.francisworld.fraudstream.model.Transaction;
import dev.francisworld.fraudstream.schema.DecodeException;
import dev.francisworld.fraudstream.schema.TransactionCodec;
import dev.francisworld.fraudstream.serde.Json;
import dev.francisworld.fraudstream.serde.JsonSerde;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * Assembles the whole fraud pipeline.
 *
 * <pre>
 * transactions -> decode -> [dlq]
 *              -> by-account: dedupe, grace check, velocity, travel -> [late]
 *              -> by-merchant: amount z-score
 *              -> by-device: linked-account count (only when a device id is present)
 *              -> score with the broadcast rule set -> alerts -> by-account
 *                    -> alerts topic, account-risk table, case management
 * case-commands -> decode -> [dlq] -> by-account -> case management -> case-events
 * </pre>
 */
public final class FraudTopology {

  private final Topics topics;
  private final FraudSettings settings;
  private final FraudMetrics metrics;
  private final TransactionCodec codec;

  public FraudTopology(
      Topics topics, FraudSettings settings, FraudMetrics metrics, TransactionCodec codec) {
    this.topics = topics;
    this.settings = settings;
    this.metrics = metrics;
    this.codec = codec;
  }

  public Topology build() {
    StreamsBuilder builder = new StreamsBuilder();
    registerStores(builder);

    var strings = Serdes.String();
    var transactionSerde = JsonSerde.of(Transaction.class);
    var enrichedSerde = JsonSerde.of(EnrichedTransaction.class);
    var alertSerde = JsonSerde.of(Alert.class);
    var commandSerde = JsonSerde.of(CaseCommand.class);

    builder.addGlobalStore(
        Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(StoreNames.RULES), strings, Serdes.ByteArray())
            .withLoggingDisabled(),
        topics.rules(),
        Consumed.with(strings, Serdes.ByteArray()),
        () -> new RuleStoreUpdater(metrics));

    // ---- ingestion and dead letters
    KStream<String, Decoded<Transaction>> decoded =
        builder.stream(
                topics.transactions(),
                Consumed.with(strings, Serdes.ByteArray())
                    .withTimestampExtractor(new EventTimeExtractor(codec)))
            .processValues(
                () ->
                    new DecodeProcessor<>("transactions", codec::decode, metrics::decoded, metrics),
                Named.as("decode-transactions"));
    Map<String, KStream<String, Decoded<Transaction>>> ingest =
        decoded
            .split(Named.as("ingest-"))
            .branch((k, v) -> v.isFailure(), Branched.as("failed"))
            .defaultBranch(Branched.as("ok"));
    var deadLetterSerde = JsonSerde.of(DeadLetter.class);
    ingest
        .get("ingest-failed")
        .mapValues(Decoded::failure, Named.as("transaction-dead-letters"))
        .to(topics.deadLetters(), Produced.with(strings, deadLetterSerde));

    // ---- stage 1: per-account features
    KStream<String, StageResult> accountStage =
        ingest
            .get("ingest-ok")
            .map((k, v) -> KeyValue.pair(v.value().accountId(), v.value()))
            .repartition(
                Repartitioned.<String, Transaction>as("by-account")
                    .withKeySerde(strings)
                    .withValueSerde(transactionSerde))
            .process(
                () -> new AccountFeatureProcessor(settings.grace().toMillis(), metrics),
                Named.as("account-features"),
                StoreNames.ACCOUNT_STATE);
    Map<String, KStream<String, StageResult>> graceSplit =
        accountStage
            .split(Named.as("grace-"))
            .branch((k, v) -> v instanceof StageResult.Late, Branched.as("late"))
            .defaultBranch(Branched.as("ok"));
    graceSplit
        .get("grace-late")
        .mapValues(v -> ((StageResult.Late) v).event(), Named.as("late-events"))
        .to(topics.lateEvents(), Produced.with(strings, JsonSerde.of(LateEvent.class)));

    // ---- stage 2: per-merchant baseline
    KStream<String, EnrichedTransaction> merchantStage =
        graceSplit
            .get("grace-ok")
            .map((k, v) -> {
              EnrichedTransaction e = ((StageResult.Accepted) v).transaction();
              return KeyValue.pair(e.transaction().merchantId(), e);
            })
            .repartition(
                Repartitioned.<String, EnrichedTransaction>as("by-merchant")
                    .withKeySerde(strings)
                    .withValueSerde(enrichedSerde))
            .process(
                MerchantFeatureProcessor::new,
                Named.as("merchant-features"),
                StoreNames.MERCHANT_STATS);

    // ---- stage 3: device graph, only for events that carry a device id
    Map<String, KStream<String, EnrichedTransaction>> deviceSplit =
        merchantStage
            .split(Named.as("device-"))
            .branch(
                (k, v) -> v.transaction().deviceId() != null,
                Branched.withFunction(
                    s ->
                        s.map((k, v) -> KeyValue.pair(v.transaction().deviceId(), v))
                            .repartition(
                                Repartitioned.<String, EnrichedTransaction>as("by-device")
                                    .withKeySerde(strings)
                                    .withValueSerde(enrichedSerde))
                            .process(
                                () ->
                                    new DeviceFeatureProcessor(settings.deviceWindow().toMillis()),
                                Named.as("device-features"),
                                StoreNames.DEVICE_GRAPH),
                    "graph"))
            .defaultBranch(Branched.as("none"));
    KStream<String, EnrichedTransaction> enriched =
        deviceSplit.get("device-graph").merge(deviceSplit.get("device-none"));

    // ---- stage 4: scoring with the broadcast rule set
    KStream<String, Alert> alerts =
        enriched
            .processValues(
                () -> new RuleScoringProcessor(settings.activeRuleSet(), metrics),
                Named.as("rule-scoring"))
            .map((k, v) -> KeyValue.pair(v.accountId(), v))
            .repartition(
                Repartitioned.<String, Alert>as("alerts-by-account")
                    .withKeySerde(strings)
                    .withValueSerde(alertSerde));
    alerts.to(topics.alerts(), Produced.with(strings, alertSerde));
    alerts
        .groupByKey(Grouped.with(strings, alertSerde))
        .aggregate(
            () -> AccountRisk.empty(""),
            (accountId, alert, risk) -> risk.add(alert),
            Materialized.<String, AccountRisk, KeyValueStore<Bytes, byte[]>>as(
                    StoreNames.ACCOUNT_RISK)
                .withKeySerde(strings)
                .withValueSerde(JsonSerde.of(AccountRisk.class)));

    // ---- case management
    KStream<String, Decoded<CaseCommand>> commands =
        builder.stream(topics.caseCommands(), Consumed.with(strings, Serdes.ByteArray()))
            .processValues(
                () ->
                    new DecodeProcessor<>(
                        "case-commands", FraudTopology::decodeCommand, () -> {}, metrics),
                Named.as("decode-commands"));
    Map<String, KStream<String, Decoded<CaseCommand>>> commandSplit =
        commands
            .split(Named.as("commands-"))
            .branch((k, v) -> v.isFailure(), Branched.as("failed"))
            .defaultBranch(Branched.as("ok"));
    commandSplit
        .get("commands-failed")
        .mapValues(Decoded::failure, Named.as("command-dead-letters"))
        .to(topics.deadLetters(), Produced.with(strings, deadLetterSerde));
    KStream<String, CaseInput> commandInputs =
        commandSplit
            .get("commands-ok")
            .map((k, v) -> KeyValue.pair(v.value().accountId(), v.value()))
            .repartition(
                Repartitioned.<String, CaseCommand>as("commands-by-account")
                    .withKeySerde(strings)
                    .withValueSerde(commandSerde))
            .mapValues(
                c -> (CaseInput) new CaseInput.CommandReceived(c), Named.as("wrap-commands"));
    KStream<String, CaseInput> alertInputs =
        alerts.mapValues(a -> (CaseInput) new CaseInput.AlertReceived(a), Named.as("wrap-alerts"));
    alertInputs
        .merge(commandInputs)
        .process(() -> new CaseProcessor(metrics), Named.as("case-management"), StoreNames.CASES)
        .to(topics.caseEvents(), Produced.with(strings, JsonSerde.of(CaseEvent.class)));

    return builder.build();
  }

  private static CaseCommand decodeCommand(byte[] bytes) throws DecodeException {
    if (bytes == null || bytes.length == 0) {
      throw new DecodeException("empty-payload", "Empty case command", null);
    }
    CaseCommand command;
    try {
      command = Json.MAPPER.readValue(bytes, CaseCommand.class);
    } catch (IOException e) {
      throw new DecodeException(
          "malformed-json", "Cannot parse case command: " + e.getMessage(), e);
    }
    if (command.commandId() == null
        || command.type() == null
        || command.actor() == null
        || command.caseId() == null
        || !command.caseId().startsWith(command.accountId() + "-")
        || !command.accountId().equals(AccountCases.accountOf(command.caseId()))) {
      throw new DecodeException(
          "invalid-command",
          "Command is missing fields or its case does not belong to the account",
          null);
    }
    return command;
  }

  private static void registerStores(StreamsBuilder builder) {
    builder.addStateStore(store(StoreNames.ACCOUNT_STATE, JsonSerde.of(AccountState.class)));
    builder.addStateStore(store(StoreNames.MERCHANT_STATS, JsonSerde.of(MerchantStats.class)));
    builder.addStateStore(store(StoreNames.DEVICE_GRAPH, JsonSerde.of(DeviceLinks.class)));
    builder.addStateStore(store(StoreNames.CASES, JsonSerde.of(AccountCases.class)));
  }

  private static <V> StoreBuilder<KeyValueStore<String, V>> store(
      String name, Serde<V> valueSerde) {
    KeyValueBytesStoreSupplier supplier = Stores.persistentKeyValueStore(name);
    return Stores.keyValueStoreBuilder(supplier, Serdes.String(), valueSerde);
  }
}
