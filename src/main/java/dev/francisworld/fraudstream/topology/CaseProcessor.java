package dev.francisworld.fraudstream.topology;

import dev.francisworld.fraudstream.cases.AccountCases;
import dev.francisworld.fraudstream.cases.CaseCommand;
import dev.francisworld.fraudstream.cases.CaseEvent;
import dev.francisworld.fraudstream.cases.CaseInput;
import dev.francisworld.fraudstream.cases.CaseRecord;
import dev.francisworld.fraudstream.cases.CaseStateMachine;
import dev.francisworld.fraudstream.model.Alert;
import java.util.Optional;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Event-sourced case management, keyed by account so alerts and analyst commands for one account
 * are serialised by the partition: two analysts racing to claim a case get exactly one winner.
 */
final class CaseProcessor implements Processor<String, CaseInput, String, CaseEvent> {

  private final FraudMetrics metrics;
  private ProcessorContext<String, CaseEvent> context;
  private KeyValueStore<String, AccountCases> store;

  CaseProcessor(FraudMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public void init(ProcessorContext<String, CaseEvent> context) {
    this.context = context;
    this.store = context.getStateStore(StoreNames.CASES);
  }

  @Override
  public void process(Record<String, CaseInput> record) {
    AccountCases cases = store.get(record.key());
    if (cases == null) {
      cases = AccountCases.empty(record.key());
    }
    switch (record.value()) {
      case CaseInput.AlertReceived received -> onAlert(record, cases, received.alert());
      case CaseInput.CommandReceived received -> onCommand(record, cases, received.command());
    }
  }

  private void onAlert(Record<String, CaseInput> record, AccountCases cases, Alert alert) {
    boolean duplicate = cases.cases().stream().anyMatch(c -> c.hasAlert(alert.alertId()));
    if (duplicate) {
      return;
    }
    Optional<CaseRecord> open = cases.openCase();
    if (open.isPresent()) {
      CaseRecord updated = open.get().attach(alert);
      store.put(record.key(), cases.replace(updated));
      emit(
          record,
          event(
              updated,
              CaseEvent.Type.ALERT_ATTACHED,
              alert.eventTimeMs(),
              "system",
              null,
              alert.alertId()));
      return;
    }
    CaseRecord created = CaseRecord.open(cases.nextCaseId(), alert);
    store.put(record.key(), cases.withNewCase(created));
    emit(
        record,
        event(
            created,
            CaseEvent.Type.CASE_OPENED,
            alert.eventTimeMs(),
            "system",
            null,
            alert.alertId()));
    emit(
        record,
        event(
            created,
            CaseEvent.Type.ALERT_ATTACHED,
            alert.eventTimeMs(),
            "system",
            null,
            alert.alertId()));
  }

  private void onCommand(Record<String, CaseInput> record, AccountCases cases, CaseCommand cmd) {
    Optional<CaseRecord> found = cases.find(cmd.caseId());
    if (found.isEmpty()) {
      metrics.caseCommand("rejected");
      emit(record, rejection(cmd, "CASE_NOT_FOUND"));
      return;
    }
    CaseRecord current = found.get();
    if (current.processedCommands().contains(cmd.commandId())) {
      metrics.caseCommand("duplicate");
      return;
    }
    switch (CaseStateMachine.apply(current, cmd)) {
      case CaseStateMachine.Outcome.Applied applied -> {
        store.put(record.key(), cases.replace(applied.updated()));
        metrics.caseCommand("applied");
        emit(
            record,
            event(
                applied.updated(),
                applied.event(),
                cmd.requestedAtMs(),
                cmd.actor(),
                cmd.commandId(),
                cmd.comment()));
      }
      case CaseStateMachine.Outcome.Rejected rejected -> {
        CaseRecord.HistoryEntry entry =
            new CaseRecord.HistoryEntry(
                CaseEvent.Type.COMMAND_REJECTED,
                cmd.requestedAtMs(),
                cmd.actor(),
                cmd.commandId(),
                rejected.reason());
        store.put(record.key(), cases.replace(current.rememberCommand(entry, cmd.commandId())));
        metrics.caseCommand("rejected");
        emit(record, rejection(cmd, rejected.reason()));
      }
    }
  }

  private static CaseEvent rejection(CaseCommand cmd, String reason) {
    return new CaseEvent(
        cmd.caseId(),
        cmd.accountId(),
        CaseEvent.Type.COMMAND_REJECTED,
        cmd.requestedAtMs(),
        cmd.actor(),
        cmd.commandId(),
        reason);
  }

  private static CaseEvent event(
      CaseRecord c, CaseEvent.Type type, long at, String actor, String commandId, String detail) {
    return new CaseEvent(c.caseId(), c.accountId(), type, at, actor, commandId, detail);
  }

  private void emit(Record<String, CaseInput> source, CaseEvent event) {
    context.forward(new Record<>(event.caseId(), event, source.timestamp()));
  }
}
