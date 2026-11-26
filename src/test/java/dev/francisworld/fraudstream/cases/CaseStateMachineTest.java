package dev.francisworld.fraudstream.cases;

import static org.assertj.core.api.Assertions.assertThat;

import dev.francisworld.fraudstream.model.Alert;
import dev.francisworld.fraudstream.model.RuleHit;
import dev.francisworld.fraudstream.model.Severity;
import java.util.List;
import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;

class CaseStateMachineTest {

  private static Alert alert(String id, int score) {
    return new Alert(
        id,
        "t-" + id,
        "acc-1",
        1_000,
        1,
        score,
        Severity.forScore(score),
        List.of(new RuleHit("r", score)),
        100,
        "BRL",
        "m",
        null,
        Map.of());
  }

  private static CaseCommand cmd(CommandType type, String actor, String comment) {
    return new CaseCommand(
        "c-" + System.nanoTime(), "acc-1", "acc-1-1", type, actor, comment, 2_000);
  }

  private static CaseRecord fresh() {
    return CaseRecord.open("acc-1-1", alert("a1", 50));
  }

  private static CaseRecord applied(CaseRecord c, CaseCommand cmd) {
    return ((CaseStateMachine.Outcome.Applied) CaseStateMachine.apply(c, cmd)).updated();
  }

  private static String rejection(CaseRecord c, CaseCommand cmd) {
    return ((CaseStateMachine.Outcome.Rejected) CaseStateMachine.apply(c, cmd)).reason();
  }

  @Test
  void claimThenCloseHappyPath() {
    CaseRecord claimed = applied(fresh(), cmd(CommandType.CLAIM, "alice", null));
    assertThat(claimed.status()).isEqualTo(CaseStatus.IN_REVIEW);
    assertThat(claimed.assignee()).isEqualTo("alice");
    CaseRecord closed = applied(claimed, cmd(CommandType.DISMISS, "alice", "false positive"));
    assertThat(closed.status()).isEqualTo(CaseStatus.DISMISSED);
    assertThat(closed.status().closed()).isTrue();
    assertThat(closed.history())
        .extracting(CaseRecord.HistoryEntry::type)
        .containsExactly(
            CaseEvent.Type.CASE_OPENED, CaseEvent.Type.CLAIMED, CaseEvent.Type.DISMISSED);
  }

  @Test
  void releaseReturnsTheCaseToTheQueue() {
    CaseRecord claimed = applied(fresh(), cmd(CommandType.CLAIM, "alice", null));
    CaseRecord released = applied(claimed, cmd(CommandType.RELEASE, "alice", null));
    assertThat(released.status()).isEqualTo(CaseStatus.OPEN);
    assertThat(released.assignee()).isNull();
  }

  @Test
  void illegalTransitionsAreRejected() {
    CaseRecord open = fresh();
    assertThat(rejection(open, cmd(CommandType.CONFIRM_FRAUD, "alice", null)))
        .startsWith("ILLEGAL_TRANSITION");
    assertThat(rejection(open, cmd(CommandType.DISMISS, "alice", null)))
        .startsWith("ILLEGAL_TRANSITION");
    assertThat(rejection(open, cmd(CommandType.RELEASE, "alice", null)))
        .startsWith("ILLEGAL_TRANSITION");

    CaseRecord claimed = applied(open, cmd(CommandType.CLAIM, "alice", null));
    assertThat(rejection(claimed, cmd(CommandType.CLAIM, "alice", null)))
        .startsWith("ALREADY_CLAIMED");

    CaseRecord closed = applied(claimed, cmd(CommandType.CONFIRM_FRAUD, "alice", null));
    assertThat(rejection(closed, cmd(CommandType.CLAIM, "bob", null)))
        .startsWith("ILLEGAL_TRANSITION");
  }

  @Test
  void onlyTheAssigneeMayReleaseOrClose() {
    CaseRecord claimed = applied(fresh(), cmd(CommandType.CLAIM, "alice", null));
    assertThat(rejection(claimed, cmd(CommandType.RELEASE, "bob", null)))
        .startsWith("NOT_ASSIGNEE");
    assertThat(rejection(claimed, cmd(CommandType.DISMISS, "bob", null)))
        .startsWith("NOT_ASSIGNEE");
  }

  @Test
  void commentsNeedTextButWorkInAnyStatus() {
    assertThat(rejection(fresh(), cmd(CommandType.COMMENT, "alice", " ")))
        .startsWith("INVALID_COMMENT");
    assertThat(rejection(fresh(), cmd(CommandType.COMMENT, "alice", null)))
        .startsWith("INVALID_COMMENT");
    assertThat(rejection(fresh(), cmd(CommandType.COMMENT, "alice", "x".repeat(1_001))))
        .startsWith("INVALID_COMMENT");
    CaseRecord closed =
        applied(
            applied(fresh(), cmd(CommandType.CLAIM, "alice", null)),
            cmd(CommandType.DISMISS, "alice", null));
    assertThat(applied(closed, cmd(CommandType.COMMENT, "bob", "post-mortem")).status())
        .isEqualTo(CaseStatus.DISMISSED);
  }

  @Test
  void attachingAlertsRaisesMaxScoreAndIsBounded() {
    CaseRecord c = fresh();
    for (int i = 0; i < CaseRecord.MAX_ALERTS + 20; i++) {
      c = c.attach(alert("x" + i, i % 100 + 1));
    }
    assertThat(c.alerts()).hasSize(CaseRecord.MAX_ALERTS);
    assertThat(c.history()).hasSizeLessThanOrEqualTo(CaseRecord.MAX_HISTORY);
    assertThat(c.maxScore()).isEqualTo(100);
    assertThat(c.hasAlert("x119")).isTrue();
    assertThat(c.hasAlert("x0")).isFalse();
  }

  @Test
  void processedCommandIdsAreBounded() {
    CaseRecord c = fresh();
    for (int i = 0; i < CaseRecord.MAX_COMMAND_IDS + 10; i++) {
      c =
          c.rememberCommand(
              new CaseRecord.HistoryEntry(CaseEvent.Type.COMMENTED, i, "a", "id" + i, null),
              "id" + i);
    }
    assertThat(c.processedCommands())
        .hasSize(CaseRecord.MAX_COMMAND_IDS)
        .contains("id109")
        .doesNotContain("id0");
  }

  @Test
  void accountIdIsDerivedFromTheCaseId() {
    assertThat(AccountCases.accountOf("acc-eu-7-12")).isEqualTo("acc-eu-7");
    assertThat(AccountCases.accountOf("nodash")).isNull();
    assertThat(AccountCases.accountOf("-1")).isNull();
  }

  @Provide
  Arbitrary<List<CommandType>> commandSequences() {
    return Arbitraries.of(CommandType.values()).list().ofMaxSize(25);
  }

  /** Whatever sequence of commands is thrown at a case, a closed case never reopens. */
  @Property(tries = 300)
  void closedCasesStayClosed(
      @ForAll("commandSequences") List<CommandType> types, @ForAll boolean sameActor) {
    CaseRecord c = fresh();
    boolean wasClosed = false;
    int n = 0;
    for (CommandType type : types) {
      String actor = sameActor || n++ % 2 == 0 ? "alice" : "bob";
      var outcome = CaseStateMachine.apply(c, cmd(type, actor, "note"));
      if (outcome instanceof CaseStateMachine.Outcome.Applied a) {
        c = a.updated();
      }
      if (wasClosed) {
        assertThat(c.status().closed()).isTrue();
      }
      wasClosed = c.status().closed();
      if (c.status() == CaseStatus.IN_REVIEW) {
        assertThat(c.assignee()).isNotNull();
      } else if (c.status() == CaseStatus.OPEN) {
        assertThat(c.assignee()).isNull();
      }
    }
  }

  private static CaseRecord caseWith(String id, CaseStatus status) {
    CaseRecord open = CaseRecord.open(id, alert("a-" + id, 10));
    return new CaseRecord(
        id,
        "acc-1",
        status,
        status == CaseStatus.OPEN ? null : "alice",
        10,
        1,
        1,
        open.alerts(),
        open.history(),
        List.of());
  }

  @Test
  void accountsKeepAllOpenCasesAndOnlyTheMostRecentClosedOnes() {
    AccountCases cases = AccountCases.empty("acc-1");
    for (int i = 1; i <= AccountCases.MAX_CLOSED_CASES + 15; i++) {
      cases = cases.withNewCase(caseWith("acc-1-" + i, CaseStatus.DISMISSED));
    }
    assertThat(cases.cases()).hasSize(AccountCases.MAX_CLOSED_CASES);
    assertThat(cases.cases().get(0).caseId()).isEqualTo("acc-1-16");
    assertThat(cases.nextCaseId()).isEqualTo("acc-1-36");

    cases = cases.withNewCase(caseWith("acc-1-36", CaseStatus.OPEN));
    assertThat(cases.openCase()).isPresent();
    assertThat(cases.cases()).hasSize(AccountCases.MAX_CLOSED_CASES + 1);
    // closing the open case later evicts the oldest closed one, never an open one
    cases = cases.replace(caseWith("acc-1-36", CaseStatus.CONFIRMED_FRAUD));
    assertThat(cases.cases()).hasSize(AccountCases.MAX_CLOSED_CASES);
    assertThat(cases.find("acc-1-36")).isPresent();
    assertThat(cases.find("acc-1-16")).isEmpty();
  }

  @Test
  void evictionNeverDropsOpenCasesEvenWhenManyAreOpen() {
    AccountCases cases = AccountCases.empty("acc-1");
    for (int i = 1; i <= 60; i++) {
      cases =
          cases.withNewCase(
              caseWith("acc-1-" + i, i % 2 == 0 ? CaseStatus.OPEN : CaseStatus.DISMISSED));
    }
    assertThat(cases.cases().stream().filter(c -> c.status() == CaseStatus.OPEN)).hasSize(30);
    assertThat(cases.cases().stream().filter(c -> c.status().closed()))
        .hasSize(AccountCases.MAX_CLOSED_CASES);
  }
}
