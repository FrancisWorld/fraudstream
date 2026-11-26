package dev.francisworld.fraudstream.query;

import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/** {@link StateAccess} over a running {@link KafkaStreams}. */
public final class StreamsStateAccess implements StateAccess {

  private final KafkaStreams streams;
  private final HostInfo self;

  public StreamsStateAccess(KafkaStreams streams, HostInfo self) {
    this.streams = streams;
    this.self = self;
  }

  @Override
  public Placement locate(String store, String key) {
    try {
      KeyQueryMetadata metadata =
          streams.queryMetadataForKey(store, key, Serdes.String().serializer());
      if (metadata == null || metadata.equals(KeyQueryMetadata.NOT_AVAILABLE)) {
        return new Placement.Unavailable();
      }
      HostInfo host = metadata.activeHost();
      return host.equals(self)
          ? new Placement.Local()
          : new Placement.Remote(host.host(), host.port());
    } catch (IllegalStateException e) {
      return new Placement.Unavailable();
    }
  }

  @Override
  public <V> ReadOnlyKeyValueStore<String, V> store(String name) {
    try {
      return streams.store(
          StoreQueryParameters.fromNameAndType(
              name, QueryableStoreTypes.<String, V>keyValueStore()));
    } catch (InvalidStateStoreException | IllegalStateException e) {
      throw new ApiUnavailable("Store " + name + " is not queryable yet", e);
    }
  }

  @Override
  public List<Peer> peers() {
    return streams.metadataForAllStreamsClients().stream()
        .map(m -> m.hostInfo())
        .filter(h -> !h.equals(self))
        .map(h -> new Peer(h.host(), h.port()))
        .toList();
  }
}
