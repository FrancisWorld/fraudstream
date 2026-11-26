package dev.francisworld.fraudstream.model;

import java.util.List;

/** A versioned set of rules, the unit that is published and hot-reloaded. */
public record RuleSetDocument(String name, long version, List<RuleDefinition> rules) {

  public RuleSetDocument {
    rules = List.copyOf(rules);
  }
}
