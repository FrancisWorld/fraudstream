package dev.francisworld.fraudstream.api;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/** Relays a request to another instance, marking it so it cannot bounce back. */
public final class Forwarder {

  private final HopAuth hopAuth;
  private final HttpClient client =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

  public Forwarder(HopAuth hopAuth) {
    this.hopAuth = hopAuth;
  }

  public Response forward(String host, int port, Request request) {
    return send(
        host,
        port,
        request.method(),
        request.path(),
        request.rawQuery(),
        request.body(),
        request.header("authorization").orElse(null));
  }

  /** GET with the hop marker, used by fan-out queries. */
  public Response get(String host, int port, String path, String rawQuery, String authorization) {
    return send(host, port, "GET", path, rawQuery, new byte[0], authorization);
  }

  private Response send(
      String host,
      int port,
      String method,
      String path,
      String rawQuery,
      byte[] body,
      String authorization) {
    String query = rawQuery == null || rawQuery.isEmpty() ? "" : "?" + rawQuery;
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create("http://" + host + ":" + port + path + query))
            .timeout(Duration.ofSeconds(5))
            .header(HopAuth.HEADER, hopAuth.sign(method, path, rawQuery))
            .method(method, HttpRequest.BodyPublishers.ofByteArray(body));
    if (body.length > 0) {
      builder.header("Content-Type", "application/json");
    }
    if (authorization != null) {
      builder.header("Authorization", authorization);
    }
    try {
      HttpResponse<byte[]> response =
          client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
      return new Response(
          response.statusCode(),
          response.body(),
          response.headers().firstValue("Content-Type").orElse("application/json"),
          Map.of());
    } catch (IOException e) {
      throw ApiException.unavailable("Owning instance " + host + ":" + port + " is unreachable");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw ApiException.unavailable("Interrupted while contacting " + host + ":" + port);
    }
  }
}
