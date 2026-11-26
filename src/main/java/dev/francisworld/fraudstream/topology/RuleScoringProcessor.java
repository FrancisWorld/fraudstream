package dev.francisworld.fraudstream.topology;

import dev.francisworld.fraudstream.model.Alert;
import dev.francisworld.fraudstream.model.EnrichedTransaction;
import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.model.Severity;
import dev.francisworld.fraudstream.model.Transaction;
import dev.francisworld.fraudstream.rules.CompiledRuleSet;
import dev.francisworld.fraudstream.rules.RuleCompiler;
import dev.francisworld.fraudstream.rules.RuleEngine;
import dev.francisworld.fraudstream.serde.Json;
import java.util.Arrays;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates the active rule set, read from the global store on every record so a hot-reloaded
 * version applies to the very next transaction. Emits an {@link Alert} only when a rule matches.
 */
final class RuleScoringProcessor implements FixedKeyProcessor<String, EnrichedTransaction, Alert> {

  private static final Logger LOG = LoggerFactory.getLogger(RuleScoringProcessor.class);

  private final String ruleSetName;
  private final FraudMetrics metrics;
  private FixedKeyProcessorContext<String, Alert> context;
  private ReadOnlyKeyValueStore<String, byte[]> store;
  private byte[] cachedBytes;
  private CompiledRuleSet cachedRules;

  RuleScoringProcessor(String ruleSetName, FraudMetrics metrics) {
    this.ruleSetName = ruleSetName;
    this.metrics = metrics;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, Alert> context) {
    this.context = context;
    this.store = context.getStateStore(StoreNames.RULES);
  }

  @Override
  public void process(FixedKeyRecord<String, EnrichedTransaction> record) {
    CompiledRuleSet rules = activeRules();
    if (rules == null) {
      metrics.unscored();
      return;
    }
    EnrichedTransaction enriched = record.value();
    RuleEngine.Evaluation evaluation = RuleEngine.evaluate(rules, enriched);
    if (!evaluation.errors().isEmpty()) {
      metrics.ruleError(evaluation.errors().size());
      LOG.warn(
          "Rule evaluation errors for {}: {}",
          enriched.transaction().transactionId(),
          evaluation.errors());
    }
    if (!evaluation.matched()) {
      return;
    }
    Transaction tx = enriched.transaction();
    metrics.alert();
    context.forward(
        record.withValue(
            new Alert(
                Alert.idFor(tx.transactionId(), rules.version()),
                tx.transactionId(),
                tx.accountId(),
                tx.eventTimeMs(),
                rules.version(),
                evaluation.totalScore(),
                Severity.forScore(evaluation.totalScore()),
                evaluation.hits(),
                tx.amountMinor(),
                tx.currency(),
                tx.merchantId(),
                tx.deviceId(),
                enriched.features())));
  }

  private CompiledRuleSet activeRules() {
    byte[] current = store.get(ruleSetName);
    if (current == null) {
      return null;
    }
    if (cachedBytes == null || !Arrays.equals(cachedBytes, current)) {
      cachedRules = RuleCompiler.compile(Json.read(current, RuleSetDocument.class));
      cachedBytes = current;
      LOG.info("Scoring with rule set '{}' v{}", cachedRules.name(), cachedRules.version());
    }
    return cachedRules;
  }
}
