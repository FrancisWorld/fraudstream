package dev.francisworld.fraudstream.cases;

/** Immutable fact written to the case-events topic for every state change or rejection. */
public record CaseEvent(
    String caseId,
    String accountId,
    Type type,
    long timestampMs,
    String actor,
    String commandId,
    String detail) {

  /** What happened. */
  public enum Type {
    CASE_OPENED,
    ALERT_ATTACHED,
    CLAIMED,
    RELEASED,
    CONFIRMED_FRAUD,
    DISMISSED,
    COMMENTED,
    COMMAND_REJECTED
  }
}
