package dev.francisworld.fraudstream.cases;

/** Lifecycle of a fraud case. */
public enum CaseStatus {
  OPEN,
  IN_REVIEW,
  CONFIRMED_FRAUD,
  DISMISSED;

  public boolean closed() {
    return this == CONFIRMED_FRAUD || this == DISMISSED;
  }
}
