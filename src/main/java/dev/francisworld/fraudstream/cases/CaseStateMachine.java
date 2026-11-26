package dev.francisworld.fraudstream.cases;

import dev.francisworld.fraudstream.cases.CaseRecord.HistoryEntry;

/** Pure transition rules for analyst commands. */
public final class CaseStateMachine {

  static final int MAX_COMMENT = 1_000;

  private CaseStateMachine() {}

  /** Either the updated case with the event it produced, or the reason for rejection. */
  public sealed interface Outcome {
    /** The command was applied. */
    record Applied(CaseRecord updated, CaseEvent.Type event) implements Outcome {}

    /** The command broke a rule; state is unchanged. */
    record Rejected(String reason) implements Outcome {}
  }

  public static Outcome apply(CaseRecord current, CaseCommand cmd) {
    return switch (cmd.type()) {
      case CLAIM -> claim(current, cmd);
      case RELEASE -> release(current, cmd);
      case CONFIRM_FRAUD ->
          close(current, cmd, CaseStatus.CONFIRMED_FRAUD, CaseEvent.Type.CONFIRMED_FRAUD);
      case DISMISS -> close(current, cmd, CaseStatus.DISMISSED, CaseEvent.Type.DISMISSED);
      case COMMENT -> comment(current, cmd);
    };
  }

  private static Outcome claim(CaseRecord c, CaseCommand cmd) {
    if (c.status() == CaseStatus.IN_REVIEW) {
      return new Outcome.Rejected("ALREADY_CLAIMED by " + c.assignee());
    }
    if (c.status() != CaseStatus.OPEN) {
      return new Outcome.Rejected("ILLEGAL_TRANSITION: case is " + c.status());
    }
    return applied(c, cmd, CaseStatus.IN_REVIEW, cmd.actor(), CaseEvent.Type.CLAIMED);
  }

  private static Outcome release(CaseRecord c, CaseCommand cmd) {
    if (c.status() != CaseStatus.IN_REVIEW) {
      return new Outcome.Rejected("ILLEGAL_TRANSITION: case is " + c.status());
    }
    if (!cmd.actor().equals(c.assignee())) {
      return new Outcome.Rejected("NOT_ASSIGNEE: claimed by " + c.assignee());
    }
    return applied(c, cmd, CaseStatus.OPEN, null, CaseEvent.Type.RELEASED);
  }

  private static Outcome close(
      CaseRecord c, CaseCommand cmd, CaseStatus target, CaseEvent.Type event) {
    if (c.status() != CaseStatus.IN_REVIEW) {
      return new Outcome.Rejected("ILLEGAL_TRANSITION: case is " + c.status());
    }
    if (!cmd.actor().equals(c.assignee())) {
      return new Outcome.Rejected("NOT_ASSIGNEE: claimed by " + c.assignee());
    }
    return applied(c, cmd, target, c.assignee(), event);
  }

  private static Outcome comment(CaseRecord c, CaseCommand cmd) {
    if (cmd.comment() == null || cmd.comment().isBlank() || cmd.comment().length() > MAX_COMMENT) {
      return new Outcome.Rejected("INVALID_COMMENT: 1-" + MAX_COMMENT + " characters required");
    }
    return applied(c, cmd, c.status(), c.assignee(), CaseEvent.Type.COMMENTED);
  }

  private static Outcome applied(
      CaseRecord c, CaseCommand cmd, CaseStatus status, String assignee, CaseEvent.Type event) {
    HistoryEntry entry =
        new HistoryEntry(event, cmd.requestedAtMs(), cmd.actor(), cmd.commandId(), cmd.comment());
    return new Outcome.Applied(c.transition(status, assignee, entry, cmd.commandId()), event);
  }
}
