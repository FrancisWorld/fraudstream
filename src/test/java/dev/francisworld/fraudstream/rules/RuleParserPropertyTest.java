package dev.francisworld.fraudstream.rules;

import static dev.francisworld.fraudstream.support.Txs.tx;
import static org.assertj.core.api.Assertions.assertThat;

import dev.francisworld.fraudstream.model.EnrichedTransaction;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/** Properties of the DSL over randomly generated well-typed expressions. */
class RuleParserPropertyTest {

  private static final EnrichedTransaction CTX =
      EnrichedTransaction.of(tx("p").amount(77.5).build()).with("velocity_count_1m", 3);

  @Provide
  Arbitrary<String> numeric() {
    Arbitrary<String> leaf =
        Arbitraries.oneOf(
            Arbitraries.integers().between(0, 500).map(String::valueOf),
            Arbitraries.of("amount", "velocity_count_1m", "merchant_zscore", "hour_of_day"));
    return Arbitraries.recursive(
        () -> leaf,
        inner ->
            Arbitraries.oneOf(
                Combinators.binary(inner, Arbitraries.of("+", "-", "*")),
                inner.map(s -> "(-" + s + ")")),
        0,
        3);
  }

  @Provide
  Arbitrary<String> condition() {
    Arbitrary<String> cmp =
        Combinators.binary(numeric(), Arbitraries.of("<", "<=", ">", ">=", "==", "!="));
    return Arbitraries.recursive(
        () -> cmp,
        inner ->
            Arbitraries.oneOf(
                Combinators.binary(inner, Arbitraries.of("and", "or")),
                inner.map(s -> "not (" + s + ")")),
        0,
        3);
  }

  /** Printing a parsed tree and parsing it again yields the same tree text and the same value. */
  @Property(tries = 300)
  void formatIsAFixedPoint(@ForAll("condition") String source) {
    Expr first = RuleParser.parse(source);
    Expr second = RuleParser.parse(first.format());
    assertThat(second.format()).isEqualTo(first.format());
    assertThat(second.eval(CTX)).isEqualTo(first.eval(CTX));
  }

  /** Parsed arithmetic matches Java evaluation of the same operands. */
  @Property(tries = 300)
  void arithmeticMatchesJava(
      @ForAll("smallInts") int a, @ForAll("smallInts") int b, @ForAll("smallInts") int c) {
    Expr expr = RuleParser.parse(a + " + " + b + " * " + c + " == " + (a + (long) b * c));
    assertThat(expr.eval(CTX)).isEqualTo(true);
  }

  @Provide
  Arbitrary<Integer> smallInts() {
    return Arbitraries.integers().between(0, 10_000);
  }

  /** De Morgan: not (a and b) is equivalent to (not a) or (not b). */
  @Property(tries = 200)
  void deMorgan(@ForAll("condition") String a, @ForAll("condition") String b) {
    Object left = RuleParser.parse("not ((" + a + ") and (" + b + "))").eval(CTX);
    Object right = RuleParser.parse("(not (" + a + ")) or (not (" + b + "))").eval(CTX);
    assertThat(left).isEqualTo(right);
  }

  /** Small helper so generators stay readable. */
  static final class Combinators {
    private Combinators() {}

    static Arbitrary<String> binary(Arbitrary<String> operands, Arbitrary<String> operators) {
      return net.jqwik.api.Combinators.combine(operands, operators, operands)
          .as((l, op, r) -> "(" + l + " " + op + " " + r + ")");
    }
  }
}
