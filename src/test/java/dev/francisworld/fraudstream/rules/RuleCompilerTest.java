package dev.francisworld.fraudstream.rules;

import static dev.francisworld.fraudstream.support.Txs.tx;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.francisworld.fraudstream.model.EnrichedTransaction;
import dev.francisworld.fraudstream.model.RuleDefinition;
import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.support.PipelineFixture;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RuleCompilerTest {

  private static RuleSetDocument doc(String name, long version, RuleDefinition... rules) {
    return new RuleSetDocument(name, version, List.of(rules));
  }

  @Test
  void compilesTheShippedDefaultRuleSet() {
    CompiledRuleSet compiled = RuleCompiler.compile(PipelineFixture.defaultRules(4));
    assertThat(compiled.version()).isEqualTo(4);
    assertThat(compiled.rules()).hasSize(6);
  }

  @Test
  void reportsEveryProblemAtOnce() {
    RuleSetDocument bad =
        doc(
            "Bad Name",
            0,
            new RuleDefinition("ok", "", "amount > 1", 10),
            new RuleDefinition("ok", "", "amount > 2", 10),
            new RuleDefinition("Bad Id", "", "amount > 2", 10),
            new RuleDefinition("zero-score", "", "amount > 1", 0),
            new RuleDefinition("syntax", "", "amount >", 10),
            new RuleDefinition("types", "", "amount", 10));
    assertThatThrownBy(() -> RuleCompiler.compile(bad))
        .isInstanceOfSatisfying(
            RuleSetInvalidException.class,
            e ->
                assertThat(e.errors())
                    .hasSize(7)
                    .anySatisfy(m -> assertThat(m).contains("name must match"))
                    .anySatisfy(m -> assertThat(m).contains("version must be"))
                    .anySatisfy(m -> assertThat(m).contains("defined more than once"))
                    .anySatisfy(m -> assertThat(m).contains("rule id 'Bad Id'"))
                    .anySatisfy(m -> assertThat(m).contains("score must be between"))
                    .anySatisfy(m -> assertThat(m).contains("rule 'syntax'"))
                    .anySatisfy(m -> assertThat(m).contains("must evaluate to a boolean")));
  }

  @Test
  void emptyAndOversizedRuleSetsAreRejected() {
    assertThatThrownBy(() -> RuleCompiler.compile(doc("x", 1)))
        .isInstanceOf(RuleSetInvalidException.class)
        .hasMessageContaining("at least one rule");
    List<RuleDefinition> many = new ArrayList<>();
    for (int i = 0; i <= RuleCompiler.MAX_RULES; i++) {
      many.add(new RuleDefinition("r" + i, "", "amount > 1", 1));
    }
    assertThatThrownBy(() -> RuleCompiler.compile(new RuleSetDocument("x", 1, many)))
        .isInstanceOf(RuleSetInvalidException.class)
        .hasMessageContaining("at most");
  }

  @Test
  void engineSumsScoresCapsAtOneHundredAndCollectsErrors() {
    CompiledRuleSet rules =
        RuleCompiler.compile(
            doc(
                "x",
                1,
                new RuleDefinition("a", "", "amount > 1", 60),
                new RuleDefinition("b", "", "amount > 2", 60),
                new RuleDefinition("c", "", "amount > 9999", 10),
                new RuleDefinition("d", "", "amount / merchant_txn_count > 1", 10)));
    RuleEngine.Evaluation result =
        RuleEngine.evaluate(rules, EnrichedTransaction.of(tx("t").amount(50).build()));
    assertThat(result.matched()).isTrue();
    assertThat(result.hits()).extracting("ruleId").containsExactly("a", "b");
    assertThat(result.totalScore()).isEqualTo(100);
    assertThat(result.errors()).singleElement().asString().startsWith("d:");
  }

  @Test
  void noMatchMeansNoHits() {
    CompiledRuleSet rules =
        RuleCompiler.compile(doc("x", 1, new RuleDefinition("a", "", "amount > 1000", 10)));
    assertThat(RuleEngine.evaluate(rules, EnrichedTransaction.of(tx("t").build())).matched())
        .isFalse();
  }
}
