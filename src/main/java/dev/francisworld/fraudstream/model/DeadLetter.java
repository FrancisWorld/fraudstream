package dev.francisworld.fraudstream.model;

/** A record the pipeline could not process, kept with enough context to diagnose and replay it. */
public record DeadLetter(
    String reason,
    String error,
    String sourceTopic,
    int partition,
    long offset,
    long timestampMs,
    String payloadBase64) {}
