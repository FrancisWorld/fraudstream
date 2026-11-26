package dev.francisworld.fraudstream.rules;

import dev.francisworld.fraudstream.model.EnrichedTransaction;
import dev.francisworld.fraudstream.model.RuleHit;
import java.util.ArrayList;
import java.util.List;

/** Evaluates a compiled rule set against one enriched transaction. */
public final class RuleEngine {

  /** Outcome of an evaluation: matched rules, aggregate score (capped at 100) and rule errors. */
  public record Evaluation(List<RuleHit> hits, int totalScore, List<String> errors) {
    public boolean matched() {
      return !hits.isEmpty();
    }
  }

  private RuleEngine() {}

  public static Evaluation evaluate(CompiledRuleSet ruleSet, EnrichedTransaction tx) {
    List<RuleHit> hits = new ArrayList<>();
    List<String> errors = new ArrayList<>();
    int total = 0;
    for (CompiledRuleSet.CompiledRule rule : ruleSet.rules()) {
      try {
        if ((Boolean) rule.condition().eval(tx)) {
          hits.add(new RuleHit(rule.id(), rule.score()));
          total += rule.score();
        }
      } catch (RuleEvaluationException e) {
        errors.add(rule.id() + ": " + e.getMessage());
      }
    }
    return new Evaluation(hits, Math.min(100, total), errors);
  }
}
