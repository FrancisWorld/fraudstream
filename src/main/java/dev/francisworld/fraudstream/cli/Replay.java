package dev.francisworld.fraudstream.cli;

import dev.francisworld.fraudstream.model.Transaction;
import dev.francisworld.fraudstream.serde.Json;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Sends a dataset to a running instance through its public ingestion API. */
public final class Replay {

  /** Counts of what the API answered. */
  public record Summary(int sent, int accepted, int rejected, int failed) {}

  private Replay() {}

  public static Summary run(List<Labelled> data, URI apiBase, String token, int limit)
      throws InterruptedException {
    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    URI endpoint = apiBase.resolve("/api/v1/transactions");
    int sent = 0;
    int accepted = 0;
    int rejected = 0;
    int failed = 0;
    for (Labelled item : data) {
      if (limit > 0 && sent >= limit) {
        break;
      }
      sent++;
      HttpRequest request =
          HttpRequest.newBuilder(endpoint)
              .timeout(Duration.ofSeconds(10))
              .header("Authorization", "Bearer " + token)
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(toRequestJson(item.transaction())))
              .build();
      try {
        int status = client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        if (status == 202) {
          accepted++;
        } else {
          rejected++;
        }
      } catch (IOException e) {
        failed++;
      }
    }
    return new Summary(sent, accepted, rejected, failed);
  }

  static String toRequestJson(Transaction t) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("schemaVersion", 2);
    body.put("transactionId", t.transactionId());
    body.put("accountId", t.accountId());
    body.put("amountMinor", t.amountMinor());
    body.put("currency", t.currency());
    body.put("merchantId", t.merchantId());
    body.put("merchantCategory", t.merchantCategory());
    body.put("country", t.country());
    body.put("latitude", t.latitude());
    body.put("longitude", t.longitude());
    body.put("eventTime", Instant.ofEpochMilli(t.eventTimeMs()).toString());
    body.put("deviceId", t.deviceId());
    body.put("ipAddress", t.ipAddress());
    body.put("channel", t.channel());
    return Json.writeString(body);
  }
}
