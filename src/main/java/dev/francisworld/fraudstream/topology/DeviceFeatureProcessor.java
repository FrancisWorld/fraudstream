package dev.francisworld.fraudstream.topology;

import dev.francisworld.fraudstream.features.DeviceGraph;
import dev.francisworld.fraudstream.features.Features;
import dev.francisworld.fraudstream.model.DeviceLinks;
import dev.francisworld.fraudstream.model.EnrichedTransaction;
import java.util.Map;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/** Keyed by device. Maintains the device-to-account graph and counts linked accounts. */
final class DeviceFeatureProcessor
    implements Processor<String, EnrichedTransaction, String, EnrichedTransaction> {

  private final DeviceGraph graph;
  private ProcessorContext<String, EnrichedTransaction> context;
  private KeyValueStore<String, DeviceLinks> store;

  DeviceFeatureProcessor(long windowMs) {
    this.graph = new DeviceGraph(windowMs);
  }

  @Override
  public void init(ProcessorContext<String, EnrichedTransaction> context) {
    this.context = context;
    this.store = context.getStateStore(StoreNames.DEVICE_GRAPH);
  }

  @Override
  public void process(Record<String, EnrichedTransaction> record) {
    String deviceId = record.key();
    DeviceLinks previous = store.get(deviceId);
    DeviceLinks updated =
        graph.observe(
            previous == null ? new DeviceLinks(deviceId, Map.of()) : previous,
            record.value().transaction().accountId(),
            record.value().transaction().eventTimeMs());
    store.put(deviceId, updated);
    context.forward(
        record.withValue(
            record
                .value()
                .with(Features.DEVICE_ACCOUNT_COUNT, updated.accountLastSeenMs().size())));
  }
}
