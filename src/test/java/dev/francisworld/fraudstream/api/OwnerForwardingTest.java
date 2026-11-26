package dev.francisworld.fraudstream.api;

import static dev.francisworld.fraudstream.support.TestApi.ALICE_TOKEN;
import static dev.francisworld.fraudstream.support.TestApi.INGEST_TOKEN;
import static dev.francisworld.fraudstream.support.TestApi.transactionJson;
import static dev.francisworld.fraudstream.support.Txs.tx;
import static org.assertj.core.api.Assertions.assertThat;

import dev.francisworld.fraudstream.model.RuleDefinition;
import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.query.StateAccess;
import dev.francisworld.fraudstream.support.DriverStateAccess;
import dev.francisworld.fraudstream.support.PipelineFixture;
import dev.francisworld.fraudstream.support.TestApi;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Two API instances, each with its own state, wired the way a two-node deployment would be. */
class OwnerForwardingTest {

  private TestApi owner;
  private TestApi front;

  @BeforeEach
  void setUp() {
    owner = TestApi.standalone();
    owner.fixture.publishRules(
        new RuleSetDocument(
            "default", 1, List.of(new RuleDefinition("any", "", "amount >= 1", 40))));
    owner.fixture.send(tx("t-owner").account("on-owner").build());
    PipelineFixture frontFixture = new PipelineFixture();
    frontFixture.publishRules(
        new RuleSetDocument(
            "default", 1, List.of(new RuleDefinition("any", "", "amount >= 1", 40))));
    frontFixture.send(tx("t-front").account("on-front").build());
    front =
        new TestApi(
            frontFixture,
            new DriverStateAccess(
                frontFixture,
                new StateAccess.Placement.Remote("localhost", owner.http.port()),
                List.of(new StateAccess.Peer("localhost", owner.http.port()))),
            new Forwarder());
  }

  @AfterEach
  void tearDown() {
    front.close();
    owner.close();
  }

  @Test
  void keyedQueriesAreAnsweredByTheInstanceThatOwnsTheState() {
    // The front instance has no data for this account; the owner does.
    var risk = front.get("/api/v1/accounts/on-owner/risk", ALICE_TOKEN);
    assertThat(risk.status()).isEqualTo(200);
    assertThat(risk.json().get("alertCount").asInt()).isEqualTo(1);
    assertThat(front.get("/api/v1/cases/on-owner-1", ALICE_TOKEN).status()).isEqualTo(200);
    assertThat(front.get("/api/v1/cases/on-front-1", ALICE_TOKEN).status()).isEqualTo(404);
  }

  @Test
  void commandsAreForwardedAndPublishedByTheOwner() {
    var reply =
        front.call("POST", "/api/v1/cases/on-owner-1/actions", ALICE_TOKEN, "{\"type\":\"CLAIM\"}");
    assertThat(reply.status()).isEqualTo(202);
    assertThat(owner.producer.history()).hasSize(1);
    assertThat(front.producer.history()).isEmpty();
  }

  @Test
  void listQueriesFanOutToPeersAndMergeTheirCases() {
    var list = front.get("/api/v1/cases", ALICE_TOKEN).json();
    assertThat(list.get("partial").asBoolean()).isFalse();
    assertThat(list.get("cases"))
        .extracting(n -> n.get("caseId").asText())
        .containsExactlyInAnyOrder("on-front-1", "on-owner-1");
    var localOnly = front.get("/api/v1/cases?local=true", ALICE_TOKEN).json();
    assertThat(localOnly.get("cases")).hasSize(1);
  }

  @Test
  void anUnreachablePeerMarksTheListPartialInsteadOfFailing() {
    PipelineFixture lonely = new PipelineFixture();
    lonely.publishRules(
        new RuleSetDocument(
            "default", 1, List.of(new RuleDefinition("any", "", "amount >= 1", 40))));
    lonely.send(tx("t-x").account("solo").build());
    try (TestApi api =
        new TestApi(
            lonely,
            new DriverStateAccess(
                lonely,
                new StateAccess.Placement.Local(),
                List.of(new StateAccess.Peer("localhost", 1))),
            new Forwarder())) {
      var list = api.get("/api/v1/cases", ALICE_TOKEN).json();
      assertThat(list.get("partial").asBoolean()).isTrue();
      assertThat(list.get("cases")).hasSize(1);
    }
  }

  @Test
  void forwardedRequestsAreNeverForwardedAgainAndMovingStateIsRetryable() {
    var looped =
        front.call("GET", "/api/v1/accounts/x/risk", ALICE_TOKEN, null, "X-Fraudstream-Hop", "1");
    assertThat(looped.status()).isEqualTo(503);
    assertThat(looped.json().at("/error/code").asText()).isEqualTo("unavailable");
  }

  @Test
  void unavailablePlacementIsServiceUnavailable() {
    PipelineFixture f = new PipelineFixture();
    try (TestApi api =
        new TestApi(
            f,
            new DriverStateAccess(f, new StateAccess.Placement.Unavailable(), List.of()),
            new Forwarder())) {
      assertThat(api.get("/api/v1/accounts/x/risk", ALICE_TOKEN).status()).isEqualTo(503);
      assertThat(
              api.call(
                      "POST",
                      "/api/v1/transactions",
                      INGEST_TOKEN,
                      transactionJson(tx("a").build()))
                  .status())
          .isEqualTo(202);
    }
  }

  @Test
  void anUnreachableOwnerIsServiceUnavailable() {
    PipelineFixture f = new PipelineFixture();
    try (TestApi api =
        new TestApi(
            f,
            new DriverStateAccess(f, new StateAccess.Placement.Remote("localhost", 1), List.of()),
            new Forwarder())) {
      assertThat(api.get("/api/v1/accounts/x/risk", ALICE_TOKEN).status()).isEqualTo(503);
    }
  }
}
