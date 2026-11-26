package dev.francisworld.fraudstream.rules;

import static dev.francisworld.fraudstream.support.Txs.tx;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.francisworld.fraudstream.model.EnrichedTransaction;
import dev.francisworld.fraudstream.model.Transaction;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RuleParserTest {

  private static final Transaction TX = tx("t").amount(120).merchant("m-9").country("BR").build();
  private static final EnrichedTransaction CTX =
      EnrichedTransaction.of(TX).withAll(Map.of("velocity_count_5m", 7.0, "merchant_zscore", 2.5));

  private static boolean eval(String source) {
    return (Boolean) RuleParser.parse(source).eval(CTX);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "amount > 100|true",
        "amount >= 120|true",
        "amount < 120|false",
        "amount == 120|true",
        "amount != 120|false",
        "velocity_count_5m >= 6 and amount > 100|true",
        "velocity_count_5m >= 8 or amount > 100|true",
        "not (amount > 100)|false",
        "not amount > 100 or true|true",
        "country == 'BR'|true",
        "country in ('BR', 'AR')|true",
        "country not in ('BR', 'AR')|false",
        "merchant_id == \"m-9\"|true",
        "amount * 2 + 10 == 250|true",
        "amount / 4 - 5 == 25|true",
        "-amount < 0|true",
        "amount in (120, 130)|true",
        "amount in (-1, 3.5)|false",
        "1 + 2 * 3 == 7|true",
        "(1 + 2) * 3 == 9|true"
      })
  void evaluatesExpressions(String source, String expected) {
    assertThat(eval(source)).isEqualTo(Boolean.parseBoolean(expected));
  }

  @Test
  void keywordsAreCaseInsensitiveAndTrueFalseAreLiterals() {
    assertThat(eval("amount > 1 AND TRUE")).isTrue();
    assertThat(eval("false OR amount > 1")).isTrue();
  }

  @Test
  void andShortCircuitsSoGuardedDivisionIsSafe() {
    assertThat(eval("merchant_txn_count > 0 and amount / merchant_txn_count > 1")).isFalse();
  }

  @Test
  void divisionByZeroFailsAtRuntime() {
    assertThatThrownBy(() -> eval("amount / merchant_txn_count > 1"))
        .isInstanceOf(RuleEvaluationException.class)
        .hasMessageContaining("division by zero");
  }

  @Test
  void zeroAndNegativeZeroAreEqual() {
    assertThat(eval("0 == -0")).isTrue();
    assertThat(eval("merchant_txn_count == -0")).isTrue();
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "amount >|Unexpected end of expression",
        "amount > 'x'|Expected number",
        "country == 5|Cannot compare",
        "amount|must evaluate to a boolean",
        "nope > 1|Unknown variable 'nope'",
        "amount > 1 and|Unexpected end",
        "(amount > 1|Expected ')'",
        "amount > 1)|Unexpected ')'",
        "country in (amount)|literals",
        "country in ('a'|Expected ')'",
        "true == true|Cannot compare",
        "amount > 1 $|Unexpected character '$'",
        "country == 'abc|Unterminated string",
        "amount > 1.2.3|Malformed number",
        "5 and true|Expected bool",
        "not 5|Expected bool",
        "country > 'a'|Expected number",
        "true in (1)|needs a number or string",
        "amount in ('a')|number literals",
        "-country == 1|Expected number"
      })
  void rejectsBadExpressionsWithPosition(String source, String message) {
    assertThatThrownBy(() -> RuleParser.parse(source))
        .isInstanceOf(RuleSyntaxException.class)
        .hasMessageContaining(message)
        .hasMessageContaining("column");
  }

  @Test
  void blankExpressionIsRejected() {
    assertThatThrownBy(() -> RuleParser.parse("  ")).isInstanceOf(RuleSyntaxException.class);
    assertThatThrownBy(() -> RuleParser.parse(null)).isInstanceOf(RuleSyntaxException.class);
  }

  @Test
  void errorPositionPointsAtTheOffendingToken() {
    assertThatThrownBy(() -> RuleParser.parse("amount > 1 and nope == 2"))
        .isInstanceOfSatisfying(
            RuleSyntaxException.class, e -> assertThat(e.position()).isEqualTo(15));
  }

  @Test
  void stringLiteralsSupportDoubledQuotes() {
    Expr expr = RuleParser.parse("country == 'it''s'");
    assertThat(expr.format()).isEqualTo("(country == 'it''s')");
    assertThat((Boolean) expr.eval(CTX)).isFalse();
  }

  @Test
  void hourOfDayComesFromEventTimeInUtc() {
    EnrichedTransaction night = EnrichedTransaction.of(tx("n").atMinute(0).build());
    assertThat((Boolean) RuleParser.parse("hour_of_day == 12").eval(night)).isTrue();
  }

  @Test
  void missingStringsReadAsEmpty() {
    assertThat(eval("device_id == ''")).isTrue();
  }

  @Test
  void catalogueExposesAllDocumentedVariables() {
    assertThat(Variables.all())
        .containsKeys(
            "amount",
            "velocity_count_5m",
            "travel_speed_kmh",
            "merchant_zscore",
            "device_account_count",
            "country");
  }
}
