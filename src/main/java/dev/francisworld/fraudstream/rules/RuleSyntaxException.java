package dev.francisworld.fraudstream.rules;

/** A rule expression that does not parse or does not type-check. */
public final class RuleSyntaxException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final int position;

  public RuleSyntaxException(String message, int position) {
    super(message + " (at column " + (position + 1) + ")");
    this.position = position;
  }

  public int position() {
    return position;
  }
}
