package dev.francisworld.fraudstream.topology;

import dev.francisworld.fraudstream.features.Features;
import dev.francisworld.fraudstream.features.MerchantStats;
import dev.francisworld.fraudstream.model.EnrichedTransaction;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/** Keyed by merchant. Scores each amount against the merchant's running baseline. */
final class MerchantFeatureProcessor
    implements Processor<String, EnrichedTransaction, String, EnrichedTransaction> {

  private ProcessorContext<String, EnrichedTransaction> context;
  private KeyValueStore<String, MerchantStats> store;

  @Override
  public void init(ProcessorContext<String, EnrichedTransaction> context) {
    this.context = context;
    this.store = context.getStateStore(StoreNames.MERCHANT_STATS);
  }

  @Override
  public void process(Record<String, EnrichedTransaction> record) {
    MerchantStats stats = store.get(record.key());
    MerchantStats.Scored scored =
        (stats == null ? MerchantStats.empty() : stats)
            .observe(record.value().transaction().amountMinor());
    store.put(record.key(), scored.stats());
    EnrichedTransaction enriched =
        record
            .value()
            .with(Features.MERCHANT_ZSCORE, scored.zscore())
            .with(Features.MERCHANT_TXN_COUNT, scored.stats().count());
    context.forward(record.withValue(enriched));
  }
}
