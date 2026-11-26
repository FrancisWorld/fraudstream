package dev.francisworld.fraudstream.topology;

/** Names of the topics the application reads and writes. */
public record Topics(
    String transactions,
    String rules,
    String alerts,
    String deadLetters,
    String lateEvents,
    String caseCommands,
    String caseEvents) {

  public static Topics withPrefix(String prefix) {
    return new Topics(
        prefix + ".transactions",
        prefix + ".rules",
        prefix + ".alerts",
        prefix + ".dlq",
        prefix + ".transactions.late",
        prefix + ".case-commands",
        prefix + ".case-events");
  }

  public static Topics defaults() {
    return withPrefix("fraud");
  }
}
