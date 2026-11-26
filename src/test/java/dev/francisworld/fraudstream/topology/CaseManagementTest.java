package dev.francisworld.fraudstream.topology;

import static dev.francisworld.fraudstream.support.PipelineFixture.command;
import static dev.francisworld.fraudstream.support.Txs.tx;
import static org.assertj.core.api.Assertions.assertThat;

import dev.francisworld.fraudstream.cases.CaseCommand;
import dev.francisworld.fraudstream.cases.CaseEvent;
import dev.francisworld.fraudstream.cases.CaseStatus;
import dev.francisworld.fraudstream.cases.CommandType;
import dev.francisworld.fraudstream.model.RuleDefinition;
import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.support.PipelineFixture;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CaseManagementTest {

  private PipelineFixture pipeline;

  @BeforeEach
  void setUp() {
    pipeline = new PipelineFixture();
    pipeline.publishRules(
        new RuleSetDocument(
            "default", 1, List.of(new RuleDefinition("any", "", "amount >= 1", 40))));
  }

  @AfterEach
  void tearDown() {
    pipeline.close();
  }

  private List<CaseEvent.Type> types() {
    return pipeline.caseEvents().stream().map(CaseEvent::type).toList();
  }

  @Test
  void firstAlertOpensACaseAndLaterAlertsAttachToIt() {
    pipeline.send(tx("t1").account("acc-1").atMinute(0).build());
    assertThat(types()).containsExactly(CaseEvent.Type.CASE_OPENED, CaseEvent.Type.ALERT_ATTACHED);

    pipeline.send(tx("t2").account("acc-1").atMinute(1).build());
    assertThat(types()).containsExactly(CaseEvent.Type.ALERT_ATTACHED);

    var stored = pipeline.casesStore().get("acc-1");
    assertThat(stored.cases()).hasSize(1);
    assertThat(stored.cases().get(0).caseId()).isEqualTo("acc-1-1");
    assertThat(stored.cases().get(0).alerts()).hasSize(2);
    assertThat(stored.cases().get(0).status()).isEqualTo(CaseStatus.OPEN);
  }

  @Test
  void aReplayedAlertDoesNotDuplicateWithinTheCase() {
    pipeline.send(tx("t1").account("acc-1").build());
    pipeline.caseEvents();
    // a different transaction id but identical alert id cannot occur, so replay the transaction
    pipeline.send(tx("t1").account("acc-1").build());
    assertThat(pipeline.casesStore().get("acc-1").cases().get(0).alerts()).hasSize(1);
  }

  @Test
  void onlyOneOfTwoCompetingAnalystsWinsTheClaim() {
    pipeline.send(tx("t1").account("acc-1").build());
    pipeline.caseEvents();

    pipeline.command(command("c-alice", "acc-1-1", CommandType.CLAIM, "alice", null));
    pipeline.command(command("c-bob", "acc-1-1", CommandType.CLAIM, "bob", null));

    List<CaseEvent> events = pipeline.caseEvents();
    assertThat(events)
        .extracting(CaseEvent::type)
        .containsExactly(CaseEvent.Type.CLAIMED, CaseEvent.Type.COMMAND_REJECTED);
    assertThat(events.get(1).actor()).isEqualTo("bob");
    assertThat(events.get(1).detail()).startsWith("ALREADY_CLAIMED");
    var c = pipeline.casesStore().get("acc-1").cases().get(0);
    assertThat(c.status()).isEqualTo(CaseStatus.IN_REVIEW);
    assertThat(c.assignee()).isEqualTo("alice");
  }

  @Test
  void redeliveredCommandsAreIdempotent() {
    pipeline.send(tx("t1").account("acc-1").build());
    pipeline.caseEvents();
    CaseCommand claim = command("c-1", "acc-1-1", CommandType.CLAIM, "alice", null);
    pipeline.command(claim);
    pipeline.command(claim);
    assertThat(types()).containsExactly(CaseEvent.Type.CLAIMED);
    assertThat(pipeline.counter("fraudstream.case.commands", "result", "duplicate")).isEqualTo(1);
  }

  @Test
  void fullLifecycleEndsInConfirmedFraudAndTheNextAlertOpensANewCase() {
    pipeline.send(tx("t1").account("acc-1").atMinute(0).build());
    pipeline.caseEvents();
    pipeline.command(command("c1", "acc-1-1", CommandType.CLAIM, "alice", null));
    pipeline.command(
        command("c2", "acc-1-1", CommandType.COMMENT, "alice", "cardholder unreachable"));
    pipeline.command(command("c3", "acc-1-1", CommandType.RELEASE, "alice", null));
    pipeline.command(command("c4", "acc-1-1", CommandType.CLAIM, "bob", null));
    pipeline.command(command("c5", "acc-1-1", CommandType.CONFIRM_FRAUD, "alice", null));
    pipeline.command(command("c6", "acc-1-1", CommandType.CONFIRM_FRAUD, "bob", "confirmed"));
    pipeline.command(command("c7", "acc-1-1", CommandType.CLAIM, "bob", null));

    assertThat(types())
        .containsExactly(
            CaseEvent.Type.CLAIMED,
            CaseEvent.Type.COMMENTED,
            CaseEvent.Type.RELEASED,
            CaseEvent.Type.CLAIMED,
            CaseEvent.Type.COMMAND_REJECTED,
            CaseEvent.Type.CONFIRMED_FRAUD,
            CaseEvent.Type.COMMAND_REJECTED);
    var closed = pipeline.casesStore().get("acc-1").cases().get(0);
    assertThat(closed.status()).isEqualTo(CaseStatus.CONFIRMED_FRAUD);
    assertThat(closed.history())
        .extracting(h -> h.type())
        .contains(CaseEvent.Type.COMMAND_REJECTED);

    pipeline.send(tx("t2").account("acc-1").atMinute(30).build());
    assertThat(types()).containsExactly(CaseEvent.Type.CASE_OPENED, CaseEvent.Type.ALERT_ATTACHED);
    assertThat(pipeline.casesStore().get("acc-1").cases()).hasSize(2);
    assertThat(pipeline.casesStore().get("acc-1").cases().get(1).caseId()).isEqualTo("acc-1-2");
  }

  @Test
  void commandsForUnknownCasesAreRejectedWithAnEvent() {
    pipeline.command(command("c1", "ghost-1", CommandType.CLAIM, "alice", null));
    List<CaseEvent> events = pipeline.caseEvents();
    assertThat(events).hasSize(1);
    assertThat(events.get(0).type()).isEqualTo(CaseEvent.Type.COMMAND_REJECTED);
    assertThat(events.get(0).detail()).isEqualTo("CASE_NOT_FOUND");
  }

  @Test
  void malformedOrInconsistentCommandsAreDeadLettered() {
    pipeline.sendRawCommand("acc-1", "{broken".getBytes(StandardCharsets.UTF_8));
    pipeline.command(
        new CaseCommand("c1", "acc-1", "acc-2-1", CommandType.CLAIM, "alice", null, 1L));
    pipeline.sendRawCommand("acc-1", new byte[0]);
    assertThat(pipeline.deadLetters())
        .extracting(d -> d.reason())
        .containsExactly("malformed-json", "invalid-command", "empty-payload");
    assertThat(pipeline.caseEvents()).isEmpty();
  }

  @Test
  void accountIdsContainingDashesRouteCorrectly() {
    pipeline.send(tx("t1").account("acc-eu-7").build());
    pipeline.caseEvents();
    pipeline.command(command("c1", "acc-eu-7-1", CommandType.CLAIM, "alice", null));
    assertThat(types()).containsExactly(CaseEvent.Type.CLAIMED);
  }
}
