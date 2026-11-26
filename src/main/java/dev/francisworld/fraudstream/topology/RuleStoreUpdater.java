package dev.francisworld.fraudstream.topology;

import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.rules.RuleCompiler;
import dev.francisworld.fraudstream.serde.Json;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Feeds the global rules store, which every stream thread of every instance sees (the broadcast). A
 * rule set replaces the current one only if it is valid and carries a strictly higher version, so
 * stale re-deliveries and broken rules never take effect.
 */
final class RuleStoreUpdater implements Processor<String, byte[], Void, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(RuleStoreUpdater.class);

  private final FraudMetrics metrics;
  private KeyValueStore<String, byte[]> store;

  RuleStoreUpdater(FraudMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public void init(ProcessorContext<Void, Void> context) {
    this.store = context.getStateStore(StoreNames.RULES);
  }

  @Override
  public void process(Record<String, byte[]> record) {
    if (record.value() == null) {
      metrics.rulesReload("ignored-tombstone");
      return;
    }
    RuleSetDocument incoming;
    try {
      incoming = Json.read(record.value(), RuleSetDocument.class);
      RuleCompiler.compile(incoming);
    } catch (RuntimeException e) {
      metrics.rulesReload("rejected-invalid");
      LOG.warn("Ignoring invalid rule set update: {}", e.getMessage());
      return;
    }
    byte[] currentBytes = store.get(incoming.name());
    if (currentBytes != null) {
      long currentVersion = Json.read(currentBytes, RuleSetDocument.class).version();
      if (incoming.version() <= currentVersion) {
        metrics.rulesReload("ignored-stale");
        LOG.info(
            "Ignoring rule set '{}' v{}: current version is v{}",
            incoming.name(),
            incoming.version(),
            currentVersion);
        return;
      }
    }
    store.put(incoming.name(), record.value());
    metrics.rulesReload("applied");
    LOG.info("Activated rule set '{}' v{}", incoming.name(), incoming.version());
  }
}
