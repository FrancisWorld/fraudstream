package dev.francisworld.fraudstream.serde;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.io.UncheckedIOException;

/** Shared, strictly configured Jackson mapper. */
public final class Json {

  public static final ObjectMapper MAPPER =
      JsonMapper.builder()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
          .defaultPropertyInclusion(
              JsonInclude.Value.construct(
                  JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL))
          .build();

  private Json() {}

  public static byte[] write(Object value) {
    try {
      return MAPPER.writeValueAsBytes(value);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static String writeString(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static <T> T read(byte[] bytes, Class<T> type) {
    try {
      return MAPPER.readValue(bytes, type);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
