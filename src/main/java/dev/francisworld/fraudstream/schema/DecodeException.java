package dev.francisworld.fraudstream.schema;

/** Thrown when bytes cannot be turned into a valid domain object. */
public final class DecodeException extends Exception {
  private static final long serialVersionUID = 1L;

  private final String reason;

  public DecodeException(String reason, String message, Throwable cause) {
    super(message, cause);
    this.reason = reason;
  }

  /** Short machine-readable category, e.g. {@code unknown-schema}. */
  public String reason() {
    return reason;
  }
}
