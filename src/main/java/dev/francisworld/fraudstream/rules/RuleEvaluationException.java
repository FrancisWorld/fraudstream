package dev.francisworld.fraudstream.rules;

/** A rule that type-checked but failed at runtime, for instance by dividing by zero. */
public final class RuleEvaluationException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public RuleEvaluationException(String message) {
    super(message);
  }
}
