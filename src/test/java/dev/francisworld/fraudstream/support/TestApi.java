package dev.francisworld.fraudstream.support;

import dev.francisworld.fraudstream.api.AccountRoutes;
import dev.francisworld.fraudstream.api.AuthTokens;
import dev.francisworld.fraudstream.api.CaseRoutes;
import dev.francisworld.fraudstream.api.Forwarder;
import dev.francisworld.fraudstream.api.HttpService;
import dev.francisworld.fraudstream.api.Router;
import dev.francisworld.fraudstream.api.RuleRoutes;
import dev.francisworld.fraudstream.api.TransactionRoutes;
import dev.francisworld.fraudstream.model.Transaction;
import dev.francisworld.fraudstream.query.StateAccess;
import dev.francisworld.fraudstream.schema.TransactionRequestValidator;
import dev.francisworld.fraudstream.serde.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** A real HTTP server over the full route set, a mock producer and a topology test driver. */
public final class TestApi implements AutoCloseable {

  public static final String INGEST_TOKEN = "ingest-test-token";
  public static final String ALICE_TOKEN = "alice-test-token";
  public static final String BOB_TOKEN = "bob-test-token";
  public static final String ADMIN_TOKEN = "admin-test-token";

  public static final String TOKENS =
      "ingestor:INGEST:"
          + AuthTokens.hash(INGEST_TOKEN)
          + ",alice:ANALYST:"
          + AuthTokens.hash(ALICE_TOKEN)
          + ",bob:ANALYST:"
          + AuthTokens.hash(BOB_TOKEN)
          + ",root:ADMIN:"
          + AuthTokens.hash(ADMIN_TOKEN);

  /** Producer that can be told to fail every send. */
  public static final class FlakyProducer extends MockProducer<String, byte[]> {
    public volatile boolean failing;

    FlakyProducer() {
      super(true, null, new StringSerializer(), new ByteArraySerializer());
    }

    @Override
    public synchronized Future<RecordMetadata> send(ProducerRecord<String, byte[]> record) {
      if (failing) {
        return java.util.concurrent.CompletableFuture.failedFuture(
            new IllegalStateException("broker down"));
      }
      return super.send(record);
    }
  }

  /** Peer secret shared by every instance created in a test. */
  public static final dev.francisworld.fraudstream.api.HopAuth HOP =
      new dev.francisworld.fraudstream.api.HopAuth(
          "test-peer-secret-0123456789", Clock.systemUTC());

  public static Forwarder forwarder() {
    return new Forwarder(HOP);
  }

  public final PipelineFixture fixture;
  public final FlakyProducer producer = new FlakyProducer();
  public final HttpService http;
  private final HttpClient client = HttpClient.newHttpClient();
  private int pumped;

  public TestApi(PipelineFixture fixture, StateAccess state, Forwarder forwarder) {
    this.fixture = fixture;
    var topics = fixture.topics;
    Router router = new Router(AuthTokens.parse(TOKENS), state, forwarder, HOP);
    new TransactionRoutes(producer, topics, new TransactionRequestValidator(), fixture.codec)
        .register(router);
    new RuleRoutes(producer, topics, state, "default").register(router);
    new AccountRoutes(state).register(router);
    new CaseRoutes(
            producer,
            topics,
            state,
            forwarder,
            Clock.fixed(Instant.parse("2026-03-02T12:01:00Z"), java.time.ZoneOffset.UTC))
        .register(router);
    try {
      http = new HttpService(0, router::dispatch);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    http.start();
  }

  public static TestApi standalone() {
    PipelineFixture fixture = new PipelineFixture();
    return new TestApi(fixture, DriverStateAccess.local(fixture), forwarder());
  }

  public URI uri(String path) {
    return URI.create("http://localhost:" + http.port() + path);
  }

  /** Moves everything the API produced since the last call into the topology. */
  public void pump() {
    List<ProducerRecord<String, byte[]>> history = producer.history();
    for (; pumped < history.size(); pumped++) {
      ProducerRecord<String, byte[]> r = history.get(pumped);
      Instant ts = Instant.parse("2026-03-02T12:00:00Z");
      if (r.topic().equals(fixture.topics.transactions())) {
        try {
          ts = Instant.ofEpochMilli(fixture.codec.decode(r.value()).eventTimeMs());
        } catch (dev.francisworld.fraudstream.schema.DecodeException e) {
          throw new IllegalStateException(e);
        }
      }
      fixture.pipe(r.topic(), r.key(), r.value(), ts);
    }
  }

  public record Reply(int status, String body, java.net.http.HttpHeaders headers) {
    public <T> T as(Class<T> type) {
      return Json.read(body.getBytes(java.nio.charset.StandardCharsets.UTF_8), type);
    }

    public com.fasterxml.jackson.databind.JsonNode json() {
      try {
        return Json.MAPPER.readTree(body);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  public Reply call(String method, String path, String token, String body, String... extraHeaders) {
    HttpRequest.Builder b = HttpRequest.newBuilder(uri(path));
    b.method(
        method,
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body));
    if (token != null) {
      b.header("Authorization", "Bearer " + token);
    }
    for (int i = 0; i < extraHeaders.length; i += 2) {
      b.header(extraHeaders[i], extraHeaders[i + 1]);
    }
    try {
      HttpResponse<String> r = client.send(b.build(), HttpResponse.BodyHandlers.ofString());
      return new Reply(r.statusCode(), r.body(), r.headers());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  public Reply get(String path, String token) {
    return call("GET", path, token, null);
  }

  public static String transactionJson(Transaction t) {
    return String.format(
        "{\"schemaVersion\":2,\"transactionId\":\"%s\",\"accountId\":\"%s\",\"amountMinor\":%d,\"currency\":\"%s\","
            + "\"merchantId\":\"%s\",\"merchantCategory\":\"%s\",\"eventTime\":\"%s\"}",
        t.transactionId(),
        t.accountId(),
        t.amountMinor(),
        t.currency(),
        t.merchantId(),
        t.merchantCategory(),
        Instant.ofEpochMilli(t.eventTimeMs()));
  }

  @Override
  public void close() {
    http.close();
    fixture.close();
  }
}
