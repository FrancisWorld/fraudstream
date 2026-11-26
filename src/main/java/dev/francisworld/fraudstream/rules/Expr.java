package dev.francisworld.fraudstream.rules;

import dev.francisworld.fraudstream.model.EnrichedTransaction;
import java.util.List;
import java.util.stream.Collectors;

/** Typed expression tree produced by {@link RuleParser}. */
public sealed interface Expr {

  ValueType type();

  /** Value equality where numbers compare numerically (so 0.0 equals -0.0). */
  static boolean sameValue(Object a, Object b) {
    if (a instanceof Double x && b instanceof Double y) {
      return x.doubleValue() == y.doubleValue();
    }
    return a.equals(b);
  }

  Object eval(EnrichedTransaction tx);

  /** Fully parenthesised source form; parsing it yields an equivalent tree. */
  String format();

  record Literal(Object value, ValueType type) implements Expr {
    @Override
    public Object eval(EnrichedTransaction tx) {
      return value;
    }

    @Override
    public String format() {
      return switch (type) {
        case STRING -> "'" + ((String) value).replace("'", "''") + "'";
        case NUMBER -> numberLiteral((Double) value);
        case BOOL -> value.toString();
      };
    }

    private static String numberLiteral(double v) {
      if (v == Math.rint(v) && Math.abs(v) < 1e15) {
        return Long.toString((long) v);
      }
      return Double.toString(v);
    }
  }

  record Var(Variables.Variable variable) implements Expr {
    @Override
    public ValueType type() {
      return variable.type();
    }

    @Override
    public Object eval(EnrichedTransaction tx) {
      return variable.read(tx);
    }

    @Override
    public String format() {
      return variable.name();
    }
  }

  enum UnaryOp {
    NEGATE("-"),
    NOT("not ");

    final String symbol;

    UnaryOp(String symbol) {
      this.symbol = symbol;
    }
  }

  record Unary(UnaryOp op, Expr operand) implements Expr {
    @Override
    public ValueType type() {
      return op == UnaryOp.NOT ? ValueType.BOOL : ValueType.NUMBER;
    }

    @Override
    public Object eval(EnrichedTransaction tx) {
      Object v = operand.eval(tx);
      return op == UnaryOp.NOT ? !((Boolean) v) : -((Double) v);
    }

    @Override
    public String format() {
      return "(" + op.symbol + operand.format() + ")";
    }
  }

  enum BinaryOp {
    ADD("+", ValueType.NUMBER),
    SUB("-", ValueType.NUMBER),
    MUL("*", ValueType.NUMBER),
    DIV("/", ValueType.NUMBER),
    LT("<", ValueType.BOOL),
    LE("<=", ValueType.BOOL),
    GT(">", ValueType.BOOL),
    GE(">=", ValueType.BOOL),
    EQ("==", ValueType.BOOL),
    NE("!=", ValueType.BOOL),
    AND("and", ValueType.BOOL),
    OR("or", ValueType.BOOL);

    final String symbol;
    final ValueType result;

    BinaryOp(String symbol, ValueType result) {
      this.symbol = symbol;
      this.result = result;
    }
  }

  record Binary(BinaryOp op, Expr left, Expr right) implements Expr {
    @Override
    public ValueType type() {
      return op.result;
    }

    @Override
    public Object eval(EnrichedTransaction tx) {
      if (op == BinaryOp.AND) {
        return (Boolean) left.eval(tx) && (Boolean) right.eval(tx);
      }
      if (op == BinaryOp.OR) {
        return (Boolean) left.eval(tx) || (Boolean) right.eval(tx);
      }
      Object l = left.eval(tx);
      Object r = right.eval(tx);
      if (op == BinaryOp.EQ) {
        return sameValue(l, r);
      }
      if (op == BinaryOp.NE) {
        return !sameValue(l, r);
      }
      double a = (Double) l;
      double b = (Double) r;
      return switch (op) {
        case ADD -> a + b;
        case SUB -> a - b;
        case MUL -> a * b;
        case DIV -> {
          if (b == 0.0) {
            throw new RuleEvaluationException("division by zero in " + format());
          }
          yield a / b;
        }
        case LT -> a < b;
        case LE -> a <= b;
        case GT -> a > b;
        case GE -> a >= b;
        default -> throw new IllegalStateException("unreachable " + op);
      };
    }

    @Override
    public String format() {
      return "(" + left.format() + " " + op.symbol + " " + right.format() + ")";
    }
  }

  record InList(Expr subject, List<Literal> options, boolean negated) implements Expr {
    @Override
    public ValueType type() {
      return ValueType.BOOL;
    }

    @Override
    public Object eval(EnrichedTransaction tx) {
      Object v = subject.eval(tx);
      boolean found = options.stream().anyMatch(o -> sameValue(o.value(), v));
      return negated != found;
    }

    @Override
    public String format() {
      String list = options.stream().map(Literal::format).collect(Collectors.joining(", "));
      return "(" + subject.format() + (negated ? " not in (" : " in (") + list + "))";
    }
  }
}
