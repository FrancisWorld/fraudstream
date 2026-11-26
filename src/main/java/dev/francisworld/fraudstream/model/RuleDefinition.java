package dev.francisworld.fraudstream.model;

/** Source form of a rule: an id, a DSL condition and the score it contributes when it matches. */
public record RuleDefinition(String id, String description, String when, int score) {}
