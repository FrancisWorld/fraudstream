package dev.francisworld.fraudstream.api;

import static dev.francisworld.fraudstream.support.TestApi.ADMIN_TOKEN;
import static dev.francisworld.fraudstream.support.TestApi.ALICE_TOKEN;
import static dev.francisworld.fraudstream.support.TestApi.BOB_TOKEN;
import static dev.francisworld.fraudstream.support.TestApi.INGEST_TOKEN;
import static dev.francisworld.fraudstream.support.TestApi.transactionJson;
import static dev.francisworld.fraudstream.support.Txs.tx;
import static org.assertj.core.api.Assertions.assertThat;

import dev.francisworld.fraudstream.serde.Json;
import dev.francisworld.fraudstream.support.PipelineFixture;
import dev.francisworld.fraudstream.support.TestApi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Real HTTP, real routes, real topology and RocksDB state; only the Kafka broker is replaced. */
class ApiEndToEndTest {

  private TestApi api;

  @BeforeEach
  void setUp() {
    api = TestApi.standalone();
  }

  @AfterEach
  void tearDown() {
    api.close();
  }

  private void publishRules(long version, String when) {
    String body =
        "{\"name\":\"default\",\"version\":"
            + version
            + ",\"rules\":[{\"id\":\"any\",\"description\":\"d\",\"when\":\""
            + when
            + "\",\"score\":50}]}";
    assertThat(api.call("PUT", "/api/v1/rules", ADMIN_TOKEN, body).status()).isEqualTo(202);
    api.pump();
  }

  private void ingest(String id, String account, long minute) {
    var reply =
        api.call(
            "POST",
            "/api/v1/transactions",
            INGEST_TOKEN,
            transactionJson(tx(id).account(account).atMinute(minute).build()));
    assertThat(reply.status()).as(reply.body()).isEqualTo(202);
    api.pump();
  }

  @Test
  void authenticationAndAuthorisationAreEnforcedPerRoute() {
    assertThat(api.get("/api/v1/cases", null).status()).isEqualTo(401);
    assertThat(api.get("/api/v1/cases", null).headers().firstValue("WWW-Authenticate"))
        .contains("Bearer");
    assertThat(api.get("/api/v1/cases", "wrong").status()).isEqualTo(401);
    assertThat(api.get("/api/v1/cases", INGEST_TOKEN).status()).isEqualTo(403);
    assertThat(api.call("POST", "/api/v1/transactions", ALICE_TOKEN, "{}").status()).isEqualTo(403);
    assertThat(api.call("PUT", "/api/v1/rules", ALICE_TOKEN, "{}").status()).isEqualTo(403);
    assertThat(api.get("/api/v1/cases", ALICE_TOKEN).status()).isEqualTo(200);
    assertThat(api.get("/api/v1/cases", ADMIN_TOKEN).status()).isEqualTo(200);
    assertThat(api.call("GET", "/api/v1/cases", "x", null, "Authorization", "Basic abc").status())
        .isEqualTo(401);
  }

  @Test
  void errorsAreStructuredJson() {
    var reply = api.get("/api/v1/nope", ALICE_TOKEN);
    assertThat(reply.status()).isEqualTo(404);
    assertThat(reply.json().at("/error/code").asText()).isEqualTo("not_found");
    assertThat(reply.headers().firstValue("X-Request-Id")).isPresent();
    assertThat(api.call("DELETE", "/api/v1/cases", ALICE_TOKEN, null).status()).isEqualTo(405);
  }

  @Test
  void ingestionValidatesAndReportsEveryProblem() {
    var bad =
        api.call(
            "POST",
            "/api/v1/transactions",
            INGEST_TOKEN,
            "{\"transactionId\":\"x y\",\"amountMinor\":0}");
    assertThat(bad.status()).isEqualTo(422);
    assertThat(bad.json().at("/error/code").asText()).isEqualTo("validation_failed");
    assertThat(bad.json().at("/error/details").size()).isGreaterThan(3);
    assertThat(api.producer.history()).isEmpty();

    var ok =
        api.call("POST", "/api/v1/transactions", INGEST_TOKEN, transactionJson(tx("t-1").build()));
    assertThat(ok.status()).isEqualTo(202);
    assertThat(ok.json().get("schemaVersion").asInt()).isEqualTo(2);
    assertThat(api.producer.history())
        .singleElement()
        .satisfies(r -> {
          assertThat(r.topic()).isEqualTo("fraud.transactions");
          assertThat(r.key()).isEqualTo("acc-1");
        });
  }

  @Test
  void generationOneRequestsAreAcceptedAndStoredInTheOldSchema() throws Exception {
    String v1 =
        "{\"transactionId\":\"old-1\",\"accountId\":\"a-1\",\"amountMinor\":500,\"currency\":\"BRL\","
            + "\"merchantId\":\"m\",\"merchantCategory\":\"5411\",\"eventTime\":\"2026-03-02T12:00:00Z\"}";
    assertThat(
            api.call("POST", "/api/v1/transactions", INGEST_TOKEN, v1)
                .json()
                .get("schemaVersion")
                .asInt())
        .isEqualTo(1);
    byte[] payload = api.producer.history().get(0).value();
    assertThat(java.nio.ByteBuffer.wrap(payload, 1, 4).getInt()).isEqualTo(1);
    assertThat(api.fixture.codec.decode(payload).transactionId()).isEqualTo("old-1");
  }

  @Test
  void oversizedBodiesAreRejectedBeforeParsing() {
    var reply =
        api.call(
            "POST",
            "/api/v1/transactions",
            INGEST_TOKEN,
            "x".repeat(HttpService.MAX_BODY_BYTES + 10));
    assertThat(reply.status()).isEqualTo(413);
  }

  @Test
  void brokerFailuresBecomeServiceUnavailable() {
    api.producer.failing = true;
    var reply =
        api.call("POST", "/api/v1/transactions", INGEST_TOKEN, transactionJson(tx("t-1").build()));
    assertThat(reply.status()).isEqualTo(503);
    assertThat(
            api.call(
                    "PUT",
                    "/api/v1/rules",
                    ADMIN_TOKEN,
                    Json.writeString(PipelineFixture.defaultRules(1)))
                .status())
        .isEqualTo(503);
  }

  @Test
  void ruleSetsAreValidatedVersionedAndReadable() {
    var invalid =
        api.call(
            "PUT",
            "/api/v1/rules",
            ADMIN_TOKEN,
            "{\"name\":\"default\",\"version\":1,\"rules\":[{\"id\":\"bad\",\"when\":\"amount >\",\"score\":5}]}");
    assertThat(invalid.status()).isEqualTo(422);
    assertThat(invalid.json().at("/error/details/0").asText()).contains("rule 'bad'");
    assertThat(api.call("PUT", "/api/v1/rules", ADMIN_TOKEN, "not json").status()).isEqualTo(400);
    assertThat(api.get("/api/v1/rules", ALICE_TOKEN).status()).isEqualTo(404);
    assertThat(
            api.call(
                    "POST",
                    "/api/v1/rules/validate",
                    ADMIN_TOKEN,
                    Json.writeString(PipelineFixture.defaultRules(1)))
                .status())
        .isEqualTo(200);

    publishRules(2, "amount >= 1");
    var active = api.get("/api/v1/rules", ALICE_TOKEN);
    assertThat(active.status()).isEqualTo(200);
    assertThat(active.json().get("version").asInt()).isEqualTo(2);

    var stale =
        api.call(
            "PUT", "/api/v1/rules", ADMIN_TOKEN, Json.writeString(PipelineFixture.defaultRules(2)));
    assertThat(stale.status()).isEqualTo(409);
    assertThat(api.get("/api/v1/rules?name=other", ALICE_TOKEN).status()).isEqualTo(404);
  }

  @Test
  void anAlertFlowsFromIngestionToACaseThatAnalystsWork() {
    publishRules(1, "amount >= 1");
    ingest("t-1", "acc-1", 0);
    ingest("t-2", "acc-1", 1);

    var risk = api.get("/api/v1/accounts/acc-1/risk", ALICE_TOKEN).json();
    assertThat(risk.get("alertCount").asInt()).isEqualTo(2);
    assertThat(risk.get("maxScore").asInt()).isEqualTo(50);
    assertThat(api.get("/api/v1/accounts/quiet/risk", ALICE_TOKEN).json().get("alertCount").asInt())
        .isZero();

    var list = api.get("/api/v1/cases?status=OPEN", ALICE_TOKEN).json();
    assertThat(list.get("cases")).hasSize(1);
    assertThat(list.get("partial").asBoolean()).isFalse();
    assertThat(api.get("/api/v1/cases?status=CONFIRMED_FRAUD", ALICE_TOKEN).json().get("cases"))
        .isEmpty();
    assertThat(api.get("/api/v1/cases?status=NOPE", ALICE_TOKEN).status()).isEqualTo(400);
    assertThat(api.get("/api/v1/cases?limit=abc", ALICE_TOKEN).status()).isEqualTo(400);

    String caseId = list.at("/cases/0/caseId").asText();
    assertThat(caseId).isEqualTo("acc-1-1");
    assertThat(api.get("/api/v1/cases/" + caseId, ALICE_TOKEN).json().get("alerts")).hasSize(2);

    // The actor is the authenticated principal; a spoofed field in the body is ignored.
    var claim =
        api.call(
            "POST",
            "/api/v1/cases/" + caseId + "/actions",
            ALICE_TOKEN,
            "{\"type\":\"CLAIM\",\"actor\":\"bob\",\"commandId\":\"claim-alice-1\"}");
    assertThat(claim.status()).isEqualTo(202);
    assertThat(claim.json().get("actor").asText()).isEqualTo("alice");
    api.pump();
    var afterClaim = api.get("/api/v1/cases/" + caseId, BOB_TOKEN).json();
    assertThat(afterClaim.get("status").asText()).isEqualTo("IN_REVIEW");
    assertThat(afterClaim.get("assignee").asText()).isEqualTo("alice");
    assertThat(api.get("/api/v1/cases?assignee=alice", BOB_TOKEN).json().get("cases")).hasSize(1);
    assertThat(api.get("/api/v1/cases?accountId=other", BOB_TOKEN).json().get("cases")).isEmpty();

    var raced =
        api.call("POST", "/api/v1/cases/" + caseId + "/actions", BOB_TOKEN, "{\"type\":\"CLAIM\"}");
    assertThat(raced.status()).isEqualTo(409);
    assertThat(raced.json().at("/error/message").asText()).startsWith("ALREADY_CLAIMED");

    var replay =
        api.call(
            "POST",
            "/api/v1/cases/" + caseId + "/actions",
            ALICE_TOKEN,
            "{\"type\":\"CLAIM\",\"commandId\":\"claim-alice-1\"}");
    assertThat(replay.status()).isEqualTo(202);
    assertThat(replay.json().get("replayed").asBoolean()).isTrue();

    assertThat(
            api.call(
                    "POST",
                    "/api/v1/cases/" + caseId + "/actions",
                    ALICE_TOKEN,
                    "{\"type\":\"DISMISS\",\"comment\":\"customer confirmed\"}",
                    "Idempotency-Key",
                    "dismiss-key-1")
                .status())
        .isEqualTo(202);
    api.pump();
    assertThat(api.get("/api/v1/cases/" + caseId, ALICE_TOKEN).json().get("status").asText())
        .isEqualTo("DISMISSED");
  }

  @Test
  void caseActionsValidateTheirInput() {
    publishRules(1, "amount >= 1");
    ingest("t-1", "acc-1", 0);
    String path = "/api/v1/cases/acc-1-1/actions";
    assertThat(api.call("POST", path, ALICE_TOKEN, "nope").status()).isEqualTo(400);
    assertThat(api.call("POST", path, ALICE_TOKEN, "{}").status()).isEqualTo(400);
    assertThat(api.call("POST", path, ALICE_TOKEN, "{\"type\":\"EXPLODE\"}").status())
        .isEqualTo(400);
    assertThat(
            api.call("POST", path, ALICE_TOKEN, "{\"type\":\"CLAIM\",\"commandId\":\"bad id!\"}")
                .status())
        .isEqualTo(400);
    assertThat(
            api.call("POST", "/api/v1/cases/nodash/actions", ALICE_TOKEN, "{\"type\":\"CLAIM\"}")
                .status())
        .isEqualTo(404);
    assertThat(
            api.call("POST", "/api/v1/cases/acc-1-9/actions", ALICE_TOKEN, "{\"type\":\"CLAIM\"}")
                .status())
        .isEqualTo(404);
    assertThat(api.get("/api/v1/cases/acc-1-9", ALICE_TOKEN).status()).isEqualTo(404);
    assertThat(api.call("POST", path, ALICE_TOKEN, "{\"type\":\"COMMENT\"}").status())
        .isEqualTo(409);
  }

  @Test
  void deviceGraphIsQueryable() {
    publishRules(1, "device_account_count >= 2");
    for (int i = 0; i < 2; i++) {
      String json =
          TestApi.transactionJson(
                  tx("d-" + i).account("acc-" + i).atMinute(i).device("dev-7").build())
              .replace("\"eventTime\"", "\"deviceId\":\"dev-7\",\"eventTime\"");
      assertThat(api.call("POST", "/api/v1/transactions", INGEST_TOKEN, json).status())
          .isEqualTo(202);
      api.pump();
    }
    var links = api.get("/api/v1/devices/dev-7/accounts", ALICE_TOKEN).json();
    assertThat(links.get("accountLastSeenMs").size()).isEqualTo(2);
    assertThat(api.get("/api/v1/devices/unknown/accounts", ALICE_TOKEN).status()).isEqualTo(404);
    assertThat(api.get("/api/v1/accounts/acc-1/risk", ALICE_TOKEN).json().get("alertCount").asInt())
        .isEqualTo(1);
  }
}
