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
    Principal principal,
    boolean forwarded) {

  public String bodyText() {
    return new String(body, StandardCharsets.UTF_8);
  }

  public Optional<String> header(String name) {
    return Optional.ofNullable(headers.get(name.toLowerCase(java.util.Locale.ROOT)));
  }

  public String pathParam(String name) {
    return pathParams.get(name);
  }

  /** True only when the hop header carried a valid peer signature for this very request. */
  public boolean isForwarded() {
    return forwarded;
  }
}
