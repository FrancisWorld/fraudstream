package dev.francisworld.fraudstream.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.avro.Schema;

/**
 * Classpath-backed registry of the Avro schema generations. Ids are the generation numbers, so the
 * wire format stays self-describing without needing an external registry service.
 */
public final class SchemaRegistry {

  public static final int V1 = 1;
  public static final int V2 = 2;

  private final TreeMap<Integer, Schema> byId = new TreeMap<>();

  private SchemaRegistry() {}

  public static SchemaRegistry load() {
    SchemaRegistry registry = new SchemaRegistry();
    registry.byId.put(V1, parse("/schemas/transaction-v1.avsc"));
    registry.byId.put(V2, parse("/schemas/transaction-v2.avsc"));
    return registry;
  }

  private static Schema parse(String resource) {
    try (InputStream in = SchemaRegistry.class.getResourceAsStream(resource)) {
      if (in == null) {
        throw new IllegalStateException("Missing schema resource " + resource);
      }
      return new Schema.Parser().parse(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Optional<Schema> find(int id) {
    return Optional.ofNullable(byId.get(id));
  }

  public int latestId() {
    return byId.lastKey();
  }

  public Schema latest() {
    return byId.lastEntry().getValue();
  }

  public Map<Integer, Schema> all() {
    return java.util.Collections.unmodifiableMap(byId);
  }
}
