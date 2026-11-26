package dev.francisworld.fraudstream.cases;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** All cases of one account, the value stored per account key. */
public record AccountCases(String accountId, long nextSequence, List<CaseRecord> cases) {

  /** Closed cases kept per account, oldest evicted first. */
  public static final int MAX_CLOSED_CASES = 20;

  public AccountCases {
    cases = List.copyOf(cases);
  }

  public static AccountCases empty(String accountId) {
    return new AccountCases(accountId, 1, List.of());
  }

  public Optional<CaseRecord> find(String caseId) {
    return cases.stream().filter(c -> c.caseId().equals(caseId)).findFirst();
  }

  public Optional<CaseRecord> openCase() {
    return cases.stream().filter(c -> !c.status().closed()).findFirst();
  }

  public String nextCaseId() {
    return accountId + "-" + nextSequence;
  }

  public AccountCases withNewCase(CaseRecord created) {
    List<CaseRecord> out = new ArrayList<>(cases);
    out.add(created);
    return new AccountCases(accountId, nextSequence + 1, retain(out));
  }

  /**
   * Keeps every open case and only the most recent {@value #MAX_CLOSED_CASES} closed ones, so an
   * account that alerts for years does not grow its state record without bound. Case ids come from
   * a monotonic sequence and are never reused after eviction.
   */
  static List<CaseRecord> retain(List<CaseRecord> all) {
    long closed = all.stream().filter(c -> c.status().closed()).count();
    long toDrop = closed - MAX_CLOSED_CASES;
    if (toDrop <= 0) {
      return all;
    }
    List<CaseRecord> kept = new ArrayList<>(all.size());
    for (CaseRecord c : all) {
      if (c.status().closed() && toDrop-- > 0) {
        continue;
      }
      kept.add(c);
    }
    return kept;
  }

  public AccountCases replace(CaseRecord updated) {
    List<CaseRecord> out = new ArrayList<>(cases);
    out.replaceAll(c -> c.caseId().equals(updated.caseId()) ? updated : c);
    return new AccountCases(accountId, nextSequence, retain(out));
  }

  /** Case ids are {@code <accountId>-<sequence>}; the account id may itself contain dashes. */
  public static String accountOf(String caseId) {
    int dash = caseId.lastIndexOf('-');
    return dash <= 0 ? null : caseId.substring(0, dash);
  }
}
