package dev.francisworld.fraudstream.serde;

import java.util.Objects;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** Kafka serde that stores values as JSON. */
public final class JsonSerde<T> implements Serde<T> {

  private final Class<T> type;

  private JsonSerde(Class<T> type) {
    this.type = Objects.requireNonNull(type);
  }

  public static <T> JsonSerde<T> of(Class<T> type) {
    return new JsonSerde<>(type);
  }

  @Override
  public Serializer<T> serializer() {
    return (topic, value) -> value == null ? null : Json.write(value);
  }

  @Override
  public Deserializer<T> deserializer() {
    return (topic, bytes) -> bytes == null ? null : Json.read(bytes, type);
  }
}
