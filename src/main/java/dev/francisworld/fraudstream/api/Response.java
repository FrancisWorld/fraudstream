package dev.francisworld.fraudstream.api;

import dev.francisworld.fraudstream.serde.Json;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Outbound response. */
public record Response(int status, byte[] body, String contentType, Map<String, String> headers) {

  public static Response json(int status, Object value) {
    return new Response(status, Json.write(value), "application/json", Map.of());
  }

  public static Response text(int status, String text, String contentType) {
    return new Response(status, text.getBytes(StandardCharsets.UTF_8), contentType, Map.of());
  }

  public Response withHeader(String name, String value) {
    var merged = new java.util.HashMap<>(headers);
    merged.put(name, value);
    return new Response(status, body, contentType, Map.copyOf(merged));
  }
}
