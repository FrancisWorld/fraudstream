package dev.francisworld.fraudstream.model;

/** A transaction that arrived after its grace period had closed and was not scored. */
public record LateEvent(Transaction transaction, long streamTimeMs, long latenessMs) {}
