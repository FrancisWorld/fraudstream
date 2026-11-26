package dev.francisworld.fraudstream.api;

import java.util.List;

/** A failure that maps directly to a structured HTTP error response. */
public final class ApiException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final int status;
  private final String code;
  private final transient List<String> details;

  public ApiException(int status, String code, String message) {
    this(status, code, message, List.of());
  }

  public ApiException(int status, String code, String message, List<String> details) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = List.copyOf(details);
  }

  public int status() {
    return status;
  }

  public String code() {
    return code;
  }

  public List<String> details() {
    return details;
  }

  public static ApiException notFound(String message) {
    return new ApiException(404, "not_found", message);
  }

  public static ApiException badRequest(String message, List<String> details) {
    return new ApiException(400, "bad_request", message, details);
  }

  public static ApiException unavailable(String message) {
    return new ApiException(503, "unavailable", message);
  }
}
