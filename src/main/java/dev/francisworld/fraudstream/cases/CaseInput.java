package dev.francisworld.fraudstream.cases;

import dev.francisworld.fraudstream.model.Alert;

/** Everything the case processor consumes, merged into one ordered stream per account. */
public sealed interface CaseInput {

  /** A new alert to attach to the account's open case, or to open a case for. */
  record AlertReceived(Alert alert) implements CaseInput {}

  /** An analyst command. */
  record CommandReceived(CaseCommand command) implements CaseInput {}
}
