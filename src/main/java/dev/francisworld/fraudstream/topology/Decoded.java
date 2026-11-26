package dev.francisworld.fraudstream.topology;

import dev.francisworld.fraudstream.model.DeadLetter;

/** Either a decoded value or the dead letter explaining why decoding failed. */
public record Decoded<T>(T value, DeadLetter failure) {

  public static <T> Decoded<T> ok(T value) {
    return new Decoded<>(value, null);
  }

  public static <T> Decoded<T> failed(DeadLetter failure) {
    return new Decoded<>(null, failure);
  }

  public boolean isFailure() {
    return failure != null;
  }
}
