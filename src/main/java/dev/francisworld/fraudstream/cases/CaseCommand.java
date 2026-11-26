package dev.francisworld.fraudstream.cases;

/**
 * An analyst action. {@code actor} is set by the API from the authenticated principal, never from
 * the request body. {@code commandId} makes redelivery idempotent.
 */
public record CaseCommand(
    String commandId,
    String accountId,
    String caseId,
    CommandType type,
    String actor,
    String comment,
    long requestedAtMs) {}
