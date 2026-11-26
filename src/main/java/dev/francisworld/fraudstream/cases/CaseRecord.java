package dev.francisworld.fraudstream.cases;

import dev.francisworld.fraudstream.model.Alert;
import dev.francisworld.fraudstream.model.RuleHit;
import java.util.ArrayList;
import java.util.List;

/** The materialised state of one case, including its audit trail. */
public record CaseRecord(
    String caseId,
    String accountId,
    CaseStatus status,
    String assignee,
    int maxScore,
    long openedAtMs,
    long updatedAtMs,
    List<AlertRef> alerts,
    List<HistoryEntry> history,
    List<String> processedCommands) {

  static final int MAX_ALERTS = 100;
  static final int MAX_HISTORY = 200;
  static final int MAX_COMMAND_IDS = 100;

  /** Compact view of an alert attached to the case. */
  public record AlertRef(
      String alertId,
      String transactionId,
      int totalScore,
      long eventTimeMs,
      List<String> ruleIds) {
    static AlertRef of(Alert alert) {
      return new AlertRef(
          alert.alertId(),
          alert.transactionId(),
          alert.totalScore(),
          alert.eventTimeMs(),
          alert.hits().stream().map(RuleHit::ruleId).toList());
    }
  }

  /** One audit trail entry. */
  public record HistoryEntry(
      CaseEvent.Type type, long atMs, String actor, String commandId, String detail) {}

  public CaseRecord {
    alerts = List.copyOf(alerts);
    history = List.copyOf(history);
    processedCommands = List.copyOf(processedCommands);
  }

  public static CaseRecord open(String caseId, Alert alert) {
    return new CaseRecord(
        caseId,
        alert.accountId(),
        CaseStatus.OPEN,
        null,
        alert.totalScore(),
        alert.eventTimeMs(),
        alert.eventTimeMs(),
        List.of(AlertRef.of(alert)),
        List.of(
            new HistoryEntry(
                CaseEvent.Type.CASE_OPENED, alert.eventTimeMs(), "system", null, alert.alertId())),
        List.of());
  }

  public boolean hasAlert(String alertId) {
    return alerts.stream().anyMatch(a -> a.alertId().equals(alertId));
  }

  public CaseRecord attach(Alert alert) {
    return new CaseRecord(
        caseId,
        accountId,
        status,
        assignee,
        Math.max(maxScore, alert.totalScore()),
        openedAtMs,
        Math.max(updatedAtMs, alert.eventTimeMs()),
        capped(alerts, AlertRef.of(alert), MAX_ALERTS),
        capped(
            history,
            new HistoryEntry(
                CaseEvent.Type.ALERT_ATTACHED,
                alert.eventTimeMs(),
                "system",
                null,
                alert.alertId()),
            MAX_HISTORY),
        processedCommands);
  }

  public CaseRecord transition(
      CaseStatus newStatus, String newAssignee, HistoryEntry entry, String commandId) {
    return new CaseRecord(
        caseId,
        accountId,
        newStatus,
        newAssignee,
        maxScore,
        openedAtMs,
        Math.max(updatedAtMs, entry.atMs()),
        alerts,
        capped(history, entry, MAX_HISTORY),
        capped(processedCommands, commandId, MAX_COMMAND_IDS));
  }

  public CaseRecord rememberCommand(HistoryEntry entry, String commandId) {
    return transition(status, assignee, entry, commandId);
  }

  private static <T> List<T> capped(List<T> list, T added, int max) {
    List<T> out = new ArrayList<>(list);
    out.add(added);
    return out.size() > max ? List.copyOf(out.subList(out.size() - max, out.size())) : out;
  }
}
