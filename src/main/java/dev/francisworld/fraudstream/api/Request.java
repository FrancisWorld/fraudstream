package dev.francisworld.fraudstream.api;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/** Parsed inbound request handed to route handlers. */
public record Request(
    String method,
    String path,
    String rawQuery,
    Map<String, String> pathParams,
    Map<String, String> query,
    Map<String, String> headers,
    byte[] body,
    Principal principal) {

  public static final String HOP_HEADER = "x-fraudstream-hop";

  public String bodyText() {
    return new String(body, StandardCharsets.UTF_8);
  }

  public Optional<String> header(String name) {
    return Optional.ofNullable(headers.get(name.toLowerCase(java.util.Locale.ROOT)));
  }

  public String pathParam(String name) {
    return pathParams.get(name);
  }

  public boolean isForwarded() {
    return headers.containsKey(HOP_HEADER);
  }
}
