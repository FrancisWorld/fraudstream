package dev.francisworld.fraudstream.model;

/** One rule that matched a transaction. */
public record RuleHit(String ruleId, int score) {}
