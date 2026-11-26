package dev.francisworld.fraudstream.rules;

import java.util.List;

/** A validated rule set whose conditions are already parsed. */
public record CompiledRuleSet(String name, long version, List<CompiledRule> rules) {

  /** One parsed rule. */
  public record CompiledRule(String id, String description, Expr condition, int score) {}

  public CompiledRuleSet {
    rules = List.copyOf(rules);
  }
}
