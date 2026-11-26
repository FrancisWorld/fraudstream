package dev.francisworld.fraudstream.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Recursive-descent parser and type checker for the rule DSL.
 *
 * <pre>
 * or      := and ("or" and)*
 * and     := not ("and" not)*
 * not     := "not" not | compare
 * compare := sum [ ("=="|"!="|"&lt;"|"&lt;="|"&gt;"|"&gt;=") sum | ["not"] "in" "(" literal ("," literal)* ")" ]
 * sum     := product (("+"|"-") product)*
 * product := unary (("*"|"/") unary)*
 * unary   := "-" unary | primary
 * primary := number | string | "true" | "false" | variable | "(" or ")"
 * </pre>
 */
public final class RuleParser {

  private enum Kind {
    NUMBER,
    STRING,
    WORD,
    SYMBOL,
    END
  }

  private record Token(Kind kind, String text, int position) {}

  private final List<Token> tokens;
  private int index;

  private RuleParser(String source) {
    this.tokens = tokenize(source);
  }

  /** Parses and type-checks {@code source}; the result is always of type BOOL. */
  public static Expr parse(String source) {
    if (source == null || source.isBlank()) {
      throw new RuleSyntaxException("Expression is empty", 0);
    }
    RuleParser parser = new RuleParser(source);
    Expr expr = parser.parseOr();
    Token trailing = parser.peek();
    if (trailing.kind() != Kind.END) {
      throw new RuleSyntaxException("Unexpected '" + trailing.text() + "'", trailing.position());
    }
    if (expr.type() != ValueType.BOOL) {
      throw new RuleSyntaxException(
          "Rule must evaluate to a boolean but is " + expr.type().name().toLowerCase(Locale.ROOT),
          0);
    }
    return expr;
  }

  private static List<Token> tokenize(String s) {
    List<Token> out = new ArrayList<>();
    int i = 0;
    while (i < s.length()) {
      char c = s.charAt(i);
      if (Character.isWhitespace(c)) {
        i++;
      } else if (Character.isDigit(c) || (c == '.' && i + 1 < s.length() && isDigit(s, i + 1))) {
        int start = i;
        while (i < s.length() && (Character.isDigit(s.charAt(i)) || s.charAt(i) == '.')) {
          i++;
        }
        out.add(new Token(Kind.NUMBER, s.substring(start, i), start));
      } else if (c == '\'' || c == '"') {
        i = readString(s, i, out);
      } else if (Character.isLetter(c) || c == '_') {
        int start = i;
        while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) {
          i++;
        }
        out.add(new Token(Kind.WORD, s.substring(start, i), start));
      } else {
        i = readSymbol(s, i, out);
      }
    }
    out.add(new Token(Kind.END, "end of expression", s.length()));
    return out;
  }

  private static boolean isDigit(String s, int i) {
    return Character.isDigit(s.charAt(i));
  }

  private static int readString(String s, int start, List<Token> out) {
    char quote = s.charAt(start);
    StringBuilder text = new StringBuilder();
    int i = start + 1;
    while (i < s.length()) {
      char c = s.charAt(i);
      if (c == quote) {
        if (i + 1 < s.length() && s.charAt(i + 1) == quote) {
          text.append(quote);
          i += 2;
          continue;
        }
        out.add(new Token(Kind.STRING, text.toString(), start));
        return i + 1;
      }
      text.append(c);
      i++;
    }
    throw new RuleSyntaxException("Unterminated string literal", start);
  }

  private static int readSymbol(String s, int i, List<Token> out) {
    char c = s.charAt(i);
    String two = i + 1 < s.length() ? s.substring(i, i + 2) : "";
    if (two.equals("==") || two.equals("!=") || two.equals("<=") || two.equals(">=")) {
      out.add(new Token(Kind.SYMBOL, two, i));
      return i + 2;
    }
    if ("<>+-*/(),".indexOf(c) >= 0) {
      out.add(new Token(Kind.SYMBOL, String.valueOf(c), i));
      return i + 1;
    }
    throw new RuleSyntaxException("Unexpected character '" + c + "'", i);
  }

  private Token peek() {
    return tokens.get(index);
  }

  private Token next() {
    return tokens.get(index++);
  }

  private boolean isWord(String word) {
    Token t = peek();
    return t.kind() == Kind.WORD && t.text().equalsIgnoreCase(word);
  }

  private boolean isSymbol(String symbol) {
    Token t = peek();
    return t.kind() == Kind.SYMBOL && t.text().equals(symbol);
  }

  private void expectSymbol(String symbol) {
    if (!isSymbol(symbol)) {
      throw new RuleSyntaxException(
          "Expected '" + symbol + "' but found '" + peek().text() + "'", peek().position());
    }
    next();
  }

  private Expr parseOr() {
    Expr left = parseAnd();
    while (isWord("or")) {
      Token op = next();
      left = logical(Expr.BinaryOp.OR, left, parseAnd(), op);
    }
    return left;
  }

  private Expr parseAnd() {
    Expr left = parseNot();
    while (isWord("and")) {
      Token op = next();
      left = logical(Expr.BinaryOp.AND, left, parseNot(), op);
    }
    return left;
  }

  private Expr logical(Expr.BinaryOp op, Expr left, Expr right, Token at) {
    require(left, ValueType.BOOL, at);
    require(right, ValueType.BOOL, at);
    return new Expr.Binary(op, left, right);
  }

  private Expr parseNot() {
    if (isWord("not")) {
      Token op = next();
      Expr operand = parseNot();
      require(operand, ValueType.BOOL, op);
      return new Expr.Unary(Expr.UnaryOp.NOT, operand);
    }
    return parseCompare();
  }

  private Expr parseCompare() {
    Expr left = parseSum();
    Token t = peek();
    if (t.kind() == Kind.SYMBOL) {
      Expr.BinaryOp op =
          switch (t.text()) {
            case "==" -> Expr.BinaryOp.EQ;
            case "!=" -> Expr.BinaryOp.NE;
            case "<" -> Expr.BinaryOp.LT;
            case "<=" -> Expr.BinaryOp.LE;
            case ">" -> Expr.BinaryOp.GT;
            case ">=" -> Expr.BinaryOp.GE;
            default -> null;
          };
      if (op != null) {
        next();
        Expr right = parseSum();
        boolean ordering = op != Expr.BinaryOp.EQ && op != Expr.BinaryOp.NE;
        if (ordering) {
          require(left, ValueType.NUMBER, t);
          require(right, ValueType.NUMBER, t);
        } else if (left.type() != right.type() || left.type() == ValueType.BOOL) {
          throw new RuleSyntaxException(
              "Cannot compare " + name(left) + " with " + name(right), t.position());
        }
        return new Expr.Binary(op, left, right);
      }
    }
    boolean negated = false;
    if (isWord("not") && tokens.get(index + 1).text().equalsIgnoreCase("in")) {
      next();
      negated = true;
    }
    if (isWord("in")) {
      Token in = next();
      if (left.type() == ValueType.BOOL) {
        throw new RuleSyntaxException("'in' needs a number or string on the left", in.position());
      }
      return new Expr.InList(left, parseOptions(left.type()), negated);
    }
    return left;
  }

  private List<Expr.Literal> parseOptions(ValueType expected) {
    expectSymbol("(");
    List<Expr.Literal> options = new ArrayList<>();
    while (true) {
      Token start = peek();
      Expr item = parseUnary();
      if (!(item instanceof Expr.Literal literal) || item.type() != expected) {
        throw new RuleSyntaxException(
            "'in' list items must be " + expected.name().toLowerCase(Locale.ROOT) + " literals",
            start.position());
      }
      options.add(literal);
      if (!isSymbol(",")) {
        break;
      }
      next();
    }
    expectSymbol(")");
    return options;
  }

  private Expr parseSum() {
    Expr left = parseProduct();
    while (isSymbol("+") || isSymbol("-")) {
      Token op = next();
      Expr right = parseProduct();
      require(left, ValueType.NUMBER, op);
      require(right, ValueType.NUMBER, op);
      left =
          new Expr.Binary(
              op.text().equals("+") ? Expr.BinaryOp.ADD : Expr.BinaryOp.SUB, left, right);
    }
    return left;
  }

  private Expr parseProduct() {
    Expr left = parseUnary();
    while (isSymbol("*") || isSymbol("/")) {
      Token op = next();
      Expr right = parseUnary();
      require(left, ValueType.NUMBER, op);
      require(right, ValueType.NUMBER, op);
      left =
          new Expr.Binary(
              op.text().equals("*") ? Expr.BinaryOp.MUL : Expr.BinaryOp.DIV, left, right);
    }
    return left;
  }

  private Expr parseUnary() {
    if (isSymbol("-")) {
      Token op = next();
      Expr operand = parseUnary();
      require(operand, ValueType.NUMBER, op);
      if (operand instanceof Expr.Literal literal) {
        return new Expr.Literal(-(Double) literal.value(), ValueType.NUMBER);
      }
      return new Expr.Unary(Expr.UnaryOp.NEGATE, operand);
    }
    return parsePrimary();
  }

  private Expr parsePrimary() {
    Token t = next();
    switch (t.kind()) {
      case NUMBER:
        try {
          return new Expr.Literal(Double.parseDouble(t.text()), ValueType.NUMBER);
        } catch (NumberFormatException e) {
          throw new RuleSyntaxException("Malformed number '" + t.text() + "'", t.position());
        }
      case STRING:
        return new Expr.Literal(t.text(), ValueType.STRING);
      case WORD:
        return word(t);
      case SYMBOL:
        if (t.text().equals("(")) {
          Expr inner = parseOr();
          expectSymbol(")");
          return inner;
        }
        throw new RuleSyntaxException("Unexpected '" + t.text() + "'", t.position());
      default:
        throw new RuleSyntaxException("Unexpected end of expression", t.position());
    }
  }

  private Expr word(Token t) {
    String lower = t.text().toLowerCase(Locale.ROOT);
    if (lower.equals("true") || lower.equals("false")) {
      return new Expr.Literal(Boolean.parseBoolean(lower), ValueType.BOOL);
    }
    Variables.Variable variable = Variables.lookup(t.text());
    if (variable == null) {
      throw new RuleSyntaxException("Unknown variable '" + t.text() + "'", t.position());
    }
    return new Expr.Var(variable);
  }

  private static void require(Expr expr, ValueType type, Token at) {
    if (expr.type() != type) {
      throw new RuleSyntaxException(
          "Expected " + type.name().toLowerCase(Locale.ROOT) + " but got " + name(expr),
          at.position());
    }
  }

  private static String name(Expr expr) {
    return expr.type().name().toLowerCase(Locale.ROOT) + " '" + expr.format() + "'";
  }
}
