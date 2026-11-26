package dev.francisworld.fraudstream.query;

import java.util.List;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * Read access to the Kafka Streams state stores, aware of which instance owns which key. The
 * implementation over a live {@code KafkaStreams} and the one over a test driver share this shape.
 */
public interface StateAccess {

  /** Where a key currently lives. */
  sealed interface Placement {
    /** This instance owns the key. */
    record Local() implements Placement {}

    /** Another instance owns the key. */
    record Remote(String host, int port) implements Placement {}

    /** Metadata is not available, e.g. during a rebalance. */
    record Unavailable() implements Placement {}
  }

  Placement locate(String store, String key);

  /**
   * Local, read-only view of a store.
   *
   * @throws ApiUnavailable when the store cannot be queried right now
   */
  <V> ReadOnlyKeyValueStore<String, V> store(String name);

  /** host:port of every other running instance, for fan-out queries. */
  List<Peer> peers();

  /** Address of another instance. */
  record Peer(String host, int port) {}

  /** The store exists but cannot be read yet (starting up or rebalancing). */
  final class ApiUnavailable extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ApiUnavailable(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
