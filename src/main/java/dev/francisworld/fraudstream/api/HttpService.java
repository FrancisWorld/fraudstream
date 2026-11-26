package dev.francisworld.fraudstream.api;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** Thin JDK HttpServer wrapper: virtual threads, body size limit, request ids, graceful stop. */
public final class HttpService implements AutoCloseable {

  /** Largest accepted request body. */
  public static final int MAX_BODY_BYTES = 256 * 1024;

  /** Seconds to let in-flight requests finish on shutdown. */
  private static final int DRAIN_SECONDS = 5;

  private static final Logger LOG = LoggerFactory.getLogger(HttpService.class);

  /** Turns a raw request into a response. */
  public interface Dispatcher {
    Response dispatch(
        String method, String path, String rawQuery, Map<String, String> headers, byte[] body);
  }

  private final HttpServer server;
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

  public HttpService(int port, Dispatcher dispatcher) throws IOException {
    this.server = HttpServer.create(new InetSocketAddress(port), 128);
    server.setExecutor(executor);
    server.createContext("/", exchange -> handle(exchange, dispatcher));
  }

  public void start() {
    server.start();
    LOG.info("HTTP listening on port {}", port());
  }

  public int port() {
    return server.getAddress().getPort();
  }

  private static void handle(HttpExchange exchange, Dispatcher dispatcher) throws IOException {
    String requestId = UUID.randomUUID().toString();
    MDC.put("requestId", requestId);
    try (exchange) {
      Map<String, String> headers = new HashMap<>();
      exchange
          .getRequestHeaders()
          .forEach((k, v) -> headers.put(k.toLowerCase(java.util.Locale.ROOT), v.get(0)));
      Response response;
      byte[] body = readBody(exchange.getRequestBody());
      if (body == null) {
        response =
            Router.error(
                new ApiException(
                    413, "payload_too_large", "Body exceeds " + MAX_BODY_BYTES + " bytes"));
      } else {
        response =
            dispatcher.dispatch(
                exchange.getRequestMethod(),
                exchange.getRequestURI().getPath(),
                exchange.getRequestURI().getRawQuery(),
                headers,
                body);
      }
      exchange.getResponseHeaders().set("Content-Type", response.contentType());
      exchange.getResponseHeaders().set("X-Request-Id", requestId);
      response.headers().forEach((k, v) -> exchange.getResponseHeaders().set(k, v));
      byte[] out = response.body();
      exchange.sendResponseHeaders(response.status(), out.length == 0 ? -1 : out.length);
      if (out.length > 0) {
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(out);
        }
      }
      LOG.debug(
          "{} {} -> {}",
          exchange.getRequestMethod(),
          exchange.getRequestURI().getPath(),
          response.status());
    } finally {
      MDC.remove("requestId");
    }
  }

  /** Reads up to the limit; returns null when the body is larger. */
  private static byte[] readBody(InputStream in) throws IOException {
    byte[] data = in.readNBytes(MAX_BODY_BYTES + 1);
    return data.length > MAX_BODY_BYTES ? null : data;
  }

  @Override
  public void close() {
    server.stop(DRAIN_SECONDS);
    executor.shutdown();
  }
}
