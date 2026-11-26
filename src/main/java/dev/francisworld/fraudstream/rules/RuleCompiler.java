package dev.francisworld.fraudstream.rules;

import dev.francisworld.fraudstream.model.RuleDefinition;
import dev.francisworld.fraudstream.model.RuleSetDocument;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/** Validates a {@link RuleSetDocument} and compiles its conditions. */
public final class RuleCompiler {

  static final int MAX_RULES = 200;
  private static final Pattern RULE_ID = Pattern.compile("[a-z0-9][a-z0-9_-]{0,63}");
  private static final Pattern SET_NAME = Pattern.compile("[a-z0-9][a-z0-9_-]{0,63}");

  private RuleCompiler() {}

  /**
   * @throws RuleSetInvalidException listing every problem found
   */
  public static CompiledRuleSet compile(RuleSetDocument document) {
    List<String> errors = new ArrayList<>();
    if (document.name() == null || !SET_NAME.matcher(document.name()).matches()) {
      errors.add("name must match " + SET_NAME.pattern());
    }
    if (document.version() < 1) {
      errors.add("version must be >= 1");
    }
    if (document.rules().isEmpty()) {
      errors.add("at least one rule is required");
    }
    if (document.rules().size() > MAX_RULES) {
      errors.add("at most " + MAX_RULES + " rules are allowed");
    }
    Set<String> seen = new HashSet<>();
    List<CompiledRuleSet.CompiledRule> compiled = new ArrayList<>();
    for (RuleDefinition rule : document.rules()) {
      String label = "rule '" + rule.id() + "'";
      if (rule.id() == null || !RULE_ID.matcher(rule.id()).matches()) {
        errors.add("rule id '" + rule.id() + "' must match " + RULE_ID.pattern());
        continue;
      }
      if (!seen.add(rule.id())) {
        errors.add(label + " is defined more than once");
        continue;
      }
      if (rule.score() < 1 || rule.score() > 100) {
        errors.add(label + ": score must be between 1 and 100");
      }
      try {
        compiled.add(
            new CompiledRuleSet.CompiledRule(
                rule.id(),
                rule.description() == null ? "" : rule.description(),
                RuleParser.parse(rule.when()),
                rule.score()));
      } catch (RuleSyntaxException e) {
        errors.add(label + ": " + e.getMessage());
      }
    }
    if (!errors.isEmpty()) {
      throw new RuleSetInvalidException(errors);
    }
    return new CompiledRuleSet(document.name(), document.version(), compiled);
  }
}
