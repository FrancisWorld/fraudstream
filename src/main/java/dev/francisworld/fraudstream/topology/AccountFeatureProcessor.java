package dev.francisworld.fraudstream.topology;

import dev.francisworld.fraudstream.features.AccountFeatureTracker;
import dev.francisworld.fraudstream.features.AccountState;
import dev.francisworld.fraudstream.model.EnrichedTransaction;
import dev.francisworld.fraudstream.model.LateEvent;
import dev.francisworld.fraudstream.model.Transaction;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Keyed by account. Drops events older than {@code streamTime - grace}, ignores duplicates and
 * attaches velocity and travel features.
 */
final class AccountFeatureProcessor implements Processor<String, Transaction, String, StageResult> {

  private final AccountFeatureTracker tracker;
  private final long graceMs;
  private final FraudMetrics metrics;
  private ProcessorContext<String, StageResult> context;
  private KeyValueStore<String, AccountState> store;

  AccountFeatureProcessor(long graceMs, FraudMetrics metrics) {
    this.graceMs = graceMs;
    this.tracker = new AccountFeatureTracker(graceMs);
    this.metrics = metrics;
  }

  @Override
  public void init(ProcessorContext<String, StageResult> context) {
    this.context = context;
    this.store = context.getStateStore(StoreNames.ACCOUNT_STATE);
  }

  @Override
  public void process(Record<String, Transaction> record) {
    Transaction tx = record.value();
    long streamTime = context.currentStreamTimeMs();
    if (tx.eventTimeMs() < streamTime - graceMs) {
      metrics.late();
      context.forward(
          record.withValue(
              new StageResult.Late(new LateEvent(tx, streamTime, streamTime - tx.eventTimeMs()))));
      return;
    }
    AccountState previous = store.get(record.key());
    AccountFeatureTracker.Observation observation =
        tracker.observe(previous == null ? AccountState.empty() : previous, tx);
    if (observation.duplicate()) {
      metrics.duplicate();
      return;
    }
    store.put(record.key(), observation.state());
    context.forward(
        record.withValue(
            new StageResult.Accepted(EnrichedTransaction.of(tx).withAll(observation.features()))));
  }
}
