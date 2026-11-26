package dev.francisworld.fraudstream.topology;

import dev.francisworld.fraudstream.model.EnrichedTransaction;
import dev.francisworld.fraudstream.model.LateEvent;

/** Outcome of the per-account stage: the transaction proceeds, or it was too late. */
sealed interface StageResult {

  /** The transaction was inside its grace period and now carries account features. */
  record Accepted(EnrichedTransaction transaction) implements StageResult {}

  /** The transaction arrived after its grace period closed. */
  record Late(LateEvent event) implements StageResult {}
}
