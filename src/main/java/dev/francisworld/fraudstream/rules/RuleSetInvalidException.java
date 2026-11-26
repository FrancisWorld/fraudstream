package dev.francisworld.fraudstream.rules;

import java.util.List;

/** Carries every validation problem found in a rule set, not only the first. */
public final class RuleSetInvalidException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final transient List<String> errors;

  public RuleSetInvalidException(List<String> errors) {
    super(String.join("; ", errors));
    this.errors = List.copyOf(errors);
  }

  public List<String> errors() {
    return errors;
  }
}
