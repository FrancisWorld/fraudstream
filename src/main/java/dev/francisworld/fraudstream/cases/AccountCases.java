package dev.francisworld.fraudstream.cases;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** All cases of one account, the value stored per account key. */
public record AccountCases(String accountId, long nextSequence, List<CaseRecord> cases) {

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
    return new AccountCases(accountId, nextSequence + 1, out);
  }

  public AccountCases replace(CaseRecord updated) {
    List<CaseRecord> out = new ArrayList<>(cases);
    out.replaceAll(c -> c.caseId().equals(updated.caseId()) ? updated : c);
    return new AccountCases(accountId, nextSequence, out);
  }

  /** Case ids are {@code <accountId>-<sequence>}; the account id may itself contain dashes. */
  public static String accountOf(String caseId) {
    int dash = caseId.lastIndexOf('-');
    return dash <= 0 ? null : caseId.substring(0, dash);
  }
}
