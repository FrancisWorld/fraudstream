package dev.francisworld.fraudstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import dev.francisworld.fraudstream.api.AuthTokens;
import dev.francisworld.fraudstream.config.AppConfig;
import dev.francisworld.fraudstream.serde.Json;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The single test that needs a real broker. It is opt-in: it only runs under {@code mvn verify
 * -Psmoke}. It uses {@code FRAUDSTREAM_SMOKE_BOOTSTRAP} when set (for example the compose broker on
 * localhost:9094), otherwise it starts a KRaft broker with Testcontainers.
 *
 * <p>It exercises what TopologyTestDriver cannot: real repartition topics, exactly-once v2
 * transactions, the global rule store restored from a compacted topic, and the HTTP API in front of
 * a live Kafka Streams instance.
 */
class KafkaSmokeIT {

  private static final String INGEST = "smoke-ingest-token";
  private static final String ADMIN = "smoke-admin-token";

  @Test
  void transactionsFlowThroughRealKafkaToAlertsAndCases() throws Exception {
    String external = System.getenv("FRAUDSTREAM_SMOKE_BOOTSTRAP");
    if (external != null && !external.isBlank()) {
      run(external);
      return;
    }
    try (KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.9.1"))) {
      kafka.start();
      run(kafka.getBootstrapServers());
    }
  }

  private void run(String bootstrap) throws Exception {
    int apiPort = freePort();
    int mgmtPort = freePort();
    String suffix = Long.toString(System.nanoTime(), 36);
    Path state = Files.createTempDirectory("fraudstream-smoke");
    Map<String, String> env =
        Map.of(
            "FRAUDSTREAM_BOOTSTRAP_SERVERS",
            bootstrap,
            "FRAUDSTREAM_APPLICATION_ID",
            "smoke-" + suffix,
            "FRAUDSTREAM_TOPIC_PREFIX",
            "smoke" + suffix,
            "FRAUDSTREAM_STATE_DIR",
            state.toString(),
            "FRAUDSTREAM_HTTP_PORT",
            Integer.toString(apiPort),
            "FRAUDSTREAM_MANAGEMENT_PORT",
            Integer.toString(mgmtPort),
            "FRAUDSTREAM_PARTITIONS",
            "2",
            "FRAUDSTREAM_SEED_RULES",
            "true",
            "FRAUDSTREAM_AUTH_TOKENS",
            "ingestor:INGEST:"
                + AuthTokens.hash(INGEST)
                + ",operator:ADMIN:"
                + AuthTokens.hash(ADMIN));
    HttpClient http = HttpClient.newHttpClient();
    Application app = Application.start(AppConfig.fromEnv(env));
    try {
      await(
          "readiness",
          Duration.ofSeconds(90),
          () -> status(http, mgmtPort, "/readyz", null) == 200);
      await(
          "seeded rules",
          Duration.ofSeconds(60),
          () -> status(http, apiPort, "/api/v1/rules", ADMIN) == 200);

      Instant t0 = Instant.parse("2026-03-02T12:00:00Z");
      for (int i = 0; i < 6; i++) {
        String body = transaction("smoke-" + i, "acct-smoke", t0.plusSeconds(60L * i));
        assertThat(post(http, apiPort, "/api/v1/transactions", INGEST, body)).isEqualTo(202);
      }
      // Re-delivery of an already ingested transaction must not create a second alert.
      assertThat(
              post(
                  http,
                  apiPort,
                  "/api/v1/transactions",
                  INGEST,
                  transaction("smoke-5", "acct-smoke", t0.plusSeconds(300))))
          .isEqualTo(202);

      await(
          "account risk",
          Duration.ofSeconds(60),
          () -> {
            JsonNode risk = get(http, apiPort, "/api/v1/accounts/acct-smoke/risk", ADMIN);
            return risk != null && risk.get("alertCount").asInt() >= 1;
          });
      JsonNode cases = get(http, apiPort, "/api/v1/cases?accountId=acct-smoke", ADMIN);
      assertThat(cases.get("cases")).hasSize(1);

      List<ConsumerRecord<String, String>> alerts =
          readCommitted(bootstrap, "smoke" + suffix + ".alerts");
      assertThat(alerts).hasSize(1);
      assertThat(alerts.get(0).value())
          .contains("\"transactionId\":\"smoke-5\"")
          .contains("velocity-burst");
    } finally {
      app.close();
      Files.walk(state)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(p -> p.toFile().delete());
    }
  }

  private static String transaction(String id, String account, Instant at) {
    return "{\"schemaVersion\":2,\"transactionId\":\""
        + id
        + "\",\"accountId\":\""
        + account
        + "\",\"amountMinor\":2500,\"currency\":\"BRL\",\"merchantId\":\"m-1\",\"merchantCategory\":\"5411\",\"eventTime\":\""
        + at
        + "\"}";
  }

  private static List<ConsumerRecord<String, String>> readCommitted(
      String bootstrap, String topic) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "smoke-reader-" + System.nanoTime());
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    List<ConsumerRecord<String, String>> out = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
      List<TopicPartition> partitions =
          consumer.partitionsFor(topic).stream()
              .map(i -> new TopicPartition(topic, i.partition()))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      long deadline = System.currentTimeMillis() + 10_000;
      while (System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(500)).forEach(out::add);
      }
    }
    return out;
  }

  private static int freePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  private static int status(HttpClient http, int port, String path, String token) {
    try {
      HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path));
      if (token != null) {
        b.header("Authorization", "Bearer " + token);
      }
      return http.send(b.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    } catch (Exception e) {
      return -1;
    }
  }

  private static int post(HttpClient http, int port, String path, String token, String body)
      throws Exception {
    HttpRequest r =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .header("Authorization", "Bearer " + token)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    return http.send(r, HttpResponse.BodyHandlers.discarding()).statusCode();
  }

  private static JsonNode get(HttpClient http, int port, String path, String token) {
    try {
      HttpRequest r =
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
              .header("Authorization", "Bearer " + token)
              .build();
      HttpResponse<String> response = http.send(r, HttpResponse.BodyHandlers.ofString());
      return response.statusCode() == 200 ? Json.MAPPER.readTree(response.body()) : null;
    } catch (Exception e) {
      return null;
    }
  }

  private interface Condition {
    boolean check() throws Exception;
  }

  private static void await(String what, Duration timeout, Condition condition) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.check()) {
        return;
      }
      Thread.sleep(500);
    }
    throw new AssertionError("Timed out waiting for " + what);
  }
}
