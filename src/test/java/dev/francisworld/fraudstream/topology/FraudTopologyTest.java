package dev.francisworld.fraudstream.topology;

import static dev.francisworld.fraudstream.support.Txs.tx;
import static org.assertj.core.api.Assertions.assertThat;

import dev.francisworld.fraudstream.model.Alert;
import dev.francisworld.fraudstream.model.DeadLetter;
import dev.francisworld.fraudstream.model.RuleDefinition;
import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.model.Severity;
import dev.francisworld.fraudstream.schema.SchemaRegistry;
import dev.francisworld.fraudstream.support.PipelineFixture;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FraudTopologyTest {

  private PipelineFixture pipeline;

  @BeforeEach
  void setUp() {
    pipeline = new PipelineFixture();
  }

  @AfterEach
  void tearDown() {
    pipeline.close();
  }

  static RuleSetDocument rules(long version, String when, int score) {
    return new RuleSetDocument(
        "default", version, List.of(new RuleDefinition("test-rule", "test", when, score)));
  }

  @Test
  void velocityBurstRaisesAlertOnTheSixthTransactionInFiveMinutes() {
    pipeline.publishRules(PipelineFixture.defaultRules(1));
    for (int i = 0; i < 6; i++) {
      pipeline.send(tx("t-" + i).amount(25).atMinute(i).build());
    }
    List<Alert> alerts = pipeline.alerts();
    assertThat(alerts).hasSize(1);
    Alert alert = alerts.get(0);
    assertThat(alert.transactionId()).isEqualTo("t-5");
    assertThat(alert.hits()).extracting("ruleId").containsExactly("velocity-burst");
    assertThat(alert.totalScore()).isEqualTo(45);
    assertThat(alert.severity()).isEqualTo(Severity.MEDIUM);
    assertThat(alert.ruleSetVersion()).isEqualTo(1);
    assertThat(alert.features()).containsEntry("velocity_count_5m", 6.0);
  }

  @Test
  void slowSpendingDoesNotAlert() {
    pipeline.publishRules(PipelineFixture.defaultRules(1));
    for (int i = 0; i < 12; i++) {
      pipeline.send(tx("t-" + i).amount(25).atMinute(i * 20L).build());
    }
    assertThat(pipeline.alerts()).isEmpty();
  }

  @Test
  void impossibleTravelAcrossContinentsAlerts() {
    pipeline.publishRules(PipelineFixture.defaultRules(1));
    pipeline.send(tx("sp").account("trav").at(-23.55, -46.63).atMinute(0).build());
    pipeline.send(tx("lagos").account("trav").at(6.52, 3.38).country("NG").atMinute(10).build());
    List<Alert> alerts = pipeline.alerts();
    assertThat(alerts).hasSize(1);
    assertThat(alerts.get(0).transactionId()).isEqualTo("lagos");
    assertThat(alerts.get(0).hits()).extracting("ruleId").contains("impossible-travel");
    assertThat(alerts.get(0).features().get("travel_distance_km")).isBetween(5_000.0, 7_000.0);
  }

  @Test
  void plausibleTravelIsNotFlagged() {
    pipeline.publishRules(PipelineFixture.defaultRules(1));
    pipeline.send(tx("sp").account("flyer").at(-23.55, -46.63).atMinute(0).build());
    pipeline.send(tx("rio").account("flyer").at(-22.90, -43.17).atMinute(360).build());
    assertThat(pipeline.alerts()).isEmpty();
  }

  @Test
  void merchantAmountFarAboveBaselineAlertsOnlyAfterEnoughHistory() {
    pipeline.publishRules(PipelineFixture.defaultRules(1));
    for (int i = 0; i < 35; i++) {
      pipeline.send(
          tx("base-" + i)
              .account("buyer-" + i)
              .merchant("cafe")
              .amount(18 + (i % 9))
              .atMinute(i * 2L)
              .build());
    }
    assertThat(pipeline.alerts()).isEmpty();
    pipeline.send(tx("big").account("buyer-x").merchant("cafe").amount(900).atMinute(100).build());
    List<Alert> alerts = pipeline.alerts();
    assertThat(alerts).hasSize(1);
    assertThat(alerts.get(0).hits()).extracting("ruleId").contains("merchant-amount-anomaly");
    assertThat(alerts.get(0).features().get("merchant_zscore")).isGreaterThan(4.0);
  }

  @Test
  void deviceSharedByThreeAccountsAlerts() {
    pipeline.publishRules(PipelineFixture.defaultRules(1));
    for (int i = 0; i < 3; i++) {
      pipeline.send(
          tx("d-" + i)
              .account("ring-" + i)
              .merchant("shop-" + i)
              .device("dev-9")
              .atMinute(i)
              .build());
    }
    List<Alert> alerts = pipeline.alerts();
    assertThat(alerts).hasSize(1);
    assertThat(alerts.get(0).transactionId()).isEqualTo("d-2");
    assertThat(alerts.get(0).deviceId()).isEqualTo("dev-9");
    assertThat(alerts.get(0).hits()).extracting("ruleId").containsExactly("shared-device");
  }

  @Test
  void transactionsWithoutDeviceSkipTheGraphStage() {
    pipeline.publishRules(rules(1, "device_account_count == 0", 10));
    pipeline.send(tx("nodev").build());
    assertThat(pipeline.alerts()).hasSize(1);
  }

  @Test
  void duplicateTransactionIdIsScoredOnce() {
    pipeline.publishRules(rules(1, "amount >= 1", 10));
    pipeline.send(tx("same").build());
    pipeline.send(tx("same").build());
    assertThat(pipeline.alerts()).hasSize(1);
    assertThat(pipeline.counter("fraudstream.transactions.duplicates")).isEqualTo(1);
  }

  @Test
  void eventsBeyondGraceAreRoutedToTheLateTopicAndNotScored() {
    pipeline.publishRules(rules(1, "amount >= 1", 10));
    pipeline.send(tx("now").atMinute(30).build());
    pipeline.send(tx("too-late").atMinute(10).build());
    assertThat(pipeline.alerts()).extracting(Alert::transactionId).containsExactly("now");
    var late = pipeline.lateEvents();
    assertThat(late).hasSize(1);
    assertThat(late.get(0).transaction().transactionId()).isEqualTo("too-late");
    assertThat(late.get(0).latenessMs()).isEqualTo(Duration.ofMinutes(20).toMillis());
    assertThat(pipeline.counter("fraudstream.transactions.late")).isEqualTo(1);
  }

  @Test
  void eventsInsideGraceAreScoredAgainstTheirOwnEventTime() {
    pipeline.publishRules(rules(1, "velocity_count_1m >= 2", 10));
    pipeline.send(tx("newest").atMinute(30).build());
    pipeline.send(tx("old-1").atSecond(27 * 60).build());
    pipeline.send(tx("old-2").atSecond(27 * 60 + 30).build());
    assertThat(pipeline.lateEvents()).isEmpty();
    assertThat(pipeline.alerts()).extracting(Alert::transactionId).containsExactly("old-2");
  }

  @Test
  void graceIsConfigurable() {
    pipeline.close();
    pipeline = new PipelineFixture(PipelineFixture.settings(Duration.ofHours(1)));
    pipeline.publishRules(rules(1, "amount >= 1", 10));
    pipeline.send(tx("now").atMinute(30).build());
    pipeline.send(tx("older").atMinute(10).build());
    assertThat(pipeline.lateEvents()).isEmpty();
    assertThat(pipeline.alerts()).hasSize(2);
  }

  @Test
  void bothSchemaGenerationsAreProcessed() {
    pipeline.publishRules(rules(1, "amount >= 1", 10));
    pipeline.send(tx("v1-tx").build(), SchemaRegistry.V1);
    pipeline.send(tx("v2-tx").device("dev-1").build(), SchemaRegistry.V2);
    assertThat(pipeline.alerts())
        .extracting(Alert::transactionId)
        .containsExactlyInAnyOrder("v1-tx", "v2-tx");
  }

  @Test
  void poisonRecordsGoToTheDeadLetterTopicWithoutStallingTheStream() {
    pipeline.publishRules(rules(1, "amount >= 1", 10));
    Instant now = Instant.parse("2026-03-02T12:00:00Z");
    pipeline.sendRaw("k", "not avro".getBytes(StandardCharsets.UTF_8), now);
    pipeline.sendRaw("k", ByteBuffer.allocate(5).put((byte) 0).putInt(99).array(), now);
    pipeline.sendRaw("k", new byte[] {0, 0, 0, 0, 1, 2}, now);
    pipeline.sendRaw("k", negativeAmountPayload(), now);
    pipeline.send(tx("good").build());

    List<DeadLetter> dead = pipeline.deadLetters();
    assertThat(dead)
        .extracting(DeadLetter::reason)
        .containsExactly("bad-envelope", "unknown-schema", "corrupt-payload", "corrupt-payload");
    assertThat(dead.get(0).sourceTopic()).isEqualTo("fraud.transactions");
    assertThat(dead.get(0).payloadBase64())
        .isEqualTo(
            java.util.Base64.getEncoder()
                .encodeToString("not avro".getBytes(StandardCharsets.UTF_8)));
    assertThat(pipeline.alerts()).extracting(Alert::transactionId).containsExactly("good");
    assertThat(pipeline.counter("fraudstream.dead.letters", "reason", "unknown-schema"))
        .isEqualTo(1);
  }

  private byte[] negativeAmountPayload() {
    var schema = pipeline.registry.find(SchemaRegistry.V1).orElseThrow();
    GenericRecord r = new GenericData.Record(schema);
    r.put("transactionId", "neg");
    r.put("accountId", "acc-1");
    r.put("amountMinor", -5L);
    r.put("currency", "BRL");
    r.put("merchantId", "m-1");
    r.put("merchantCategory", "5411");
    r.put("eventTimeMs", 1L);
    try {
      var out = new java.io.ByteArrayOutputStream();
      out.write(0);
      out.writeBytes(ByteBuffer.allocate(4).putInt(SchemaRegistry.V1).array());
      BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
      new GenericDatumWriter<GenericRecord>(schema).write(r, enc);
      enc.flush();
      return out.toByteArray();
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  @Test
  void nothingIsScoredUntilARuleSetArrivesThenRulesHotReload() {
    pipeline.send(tx("a").amount(10).build());
    assertThat(pipeline.alerts()).isEmpty();
    assertThat(pipeline.counter("fraudstream.transactions.unscored")).isEqualTo(1);

    pipeline.publishRules(rules(1, "amount >= 100", 20));
    pipeline.send(tx("b").amount(150).build());
    assertThat(pipeline.alerts())
        .singleElement()
        .satisfies(a -> assertThat(a.ruleSetVersion()).isEqualTo(1));

    pipeline.publishRules(rules(2, "amount >= 1000", 20));
    pipeline.send(tx("c").amount(150).build());
    assertThat(pipeline.alerts()).isEmpty();

    pipeline.send(tx("d").amount(1500).build());
    assertThat(pipeline.alerts())
        .singleElement()
        .satisfies(a -> assertThat(a.ruleSetVersion()).isEqualTo(2));
    assertThat(pipeline.counter("fraudstream.rules.reloads", "result", "applied")).isEqualTo(2);
  }

  @Test
  void staleAndInvalidRuleSetsNeverReplaceTheActiveOne() {
    pipeline.publishRules(rules(5, "amount >= 100", 20));
    pipeline.publishRules(rules(5, "amount >= 1", 20));
    pipeline.publishRules(rules(3, "amount >= 1", 20));
    pipeline.publishRules(rules(6, "amount >= unknown_variable", 20));
    pipeline.publishRawRules("{not json".getBytes(StandardCharsets.UTF_8));

    pipeline.send(tx("small").amount(10).build());
    assertThat(pipeline.alerts()).isEmpty();
    pipeline.send(tx("large").amount(500).atMinute(1).build());
    assertThat(pipeline.alerts())
        .singleElement()
        .satisfies(a -> assertThat(a.ruleSetVersion()).isEqualTo(5));
    assertThat(pipeline.counter("fraudstream.rules.reloads", "result", "ignored-stale"))
        .isEqualTo(2);
    assertThat(pipeline.counter("fraudstream.rules.reloads", "result", "rejected-invalid"))
        .isEqualTo(2);
  }

  @Test
  void aRuleThatFailsAtRuntimeIsSkippedNotFatal() {
    RuleSetDocument doc =
        new RuleSetDocument(
            "default",
            1,
            List.of(
                new RuleDefinition(
                    "broken", "divides by zero", "amount / (velocity_count_1m - 1) > 1", 10),
                new RuleDefinition("ok", "always", "amount >= 1", 10)));
    pipeline.publishRules(doc);
    pipeline.send(tx("a").build());
    assertThat(pipeline.alerts())
        .singleElement()
        .satisfies(a -> assertThat(a.hits()).extracting("ruleId").containsExactly("ok"));
    assertThat(pipeline.counter("fraudstream.rules.evaluation.errors")).isEqualTo(1);
  }

  @Test
  void accountRiskTableAggregatesAlertsAndIgnoresNothingElse() {
    pipeline.publishRules(rules(1, "amount >= 1", 30));
    pipeline.send(tx("a").account("risky").atMinute(0).build());
    pipeline.send(tx("b").account("risky").atMinute(1).build());
    pipeline.send(tx("c").account("calm").atMinute(2).build());
    var risk = pipeline.riskStore().get("risky");
    assertThat(risk.alertCount()).isEqualTo(2);
    assertThat(risk.maxScore()).isEqualTo(30);
    assertThat(risk.lastAlertId()).isEqualTo("b@v1");
    assertThat(pipeline.riskStore().get("calm").alertCount()).isEqualTo(1);
    assertThat(pipeline.riskStore().get("nobody")).isNull();
  }

  @Test
  void scoreIsCappedAtOneHundredAndMapsToSeverity() {
    RuleSetDocument doc =
        new RuleSetDocument(
            "default",
            1,
            List.of(
                new RuleDefinition("a", "", "amount >= 1", 70),
                new RuleDefinition("b", "", "amount >= 1", 70)));
    pipeline.publishRules(doc);
    pipeline.send(tx("x").build());
    Alert alert = pipeline.alerts().get(0);
    assertThat(alert.totalScore()).isEqualTo(100);
    assertThat(alert.severity()).isEqualTo(Severity.CRITICAL);
    assertThat(Arrays.asList(Severity.values())).hasSize(4);
  }
}
