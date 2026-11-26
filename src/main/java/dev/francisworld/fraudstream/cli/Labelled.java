package dev.francisworld.fraudstream.cli;

import dev.francisworld.fraudstream.model.Transaction;

/**
 * A synthetic transaction with its ground truth. Labels are kept beside the transaction and never
 * enter the pipeline, so the topology cannot see them.
 */
public record Labelled(Transaction transaction, Label label) {

  /**
   * @param fraud whether this transaction is fraudulent
   * @param pattern the generator pattern, or {@code null} for legitimate activity
   * @param episode id shared by every transaction of one fraud episode, or {@code null}
   */
  public record Label(boolean fraud, String pattern, String episode) {
    public static Label legitimate() {
      return new Label(false, null, null);
    }
  }
}
