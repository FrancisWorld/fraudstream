package dev.francisworld.fraudstream.model;

/** Rolling per-account alert summary served by interactive queries. */
public record AccountRisk(
    String accountId, long alertCount, int maxScore, long lastAlertTimeMs, String lastAlertId) {

  public static AccountRisk empty(String accountId) {
    return new AccountRisk(accountId, 0, 0, 0, null);
  }

  public AccountRisk add(Alert alert) {
    if (alert.alertId().equals(lastAlertId)) {
      return this;
    }
    return new AccountRisk(
        alert.accountId(),
        alertCount + 1,
        Math.max(maxScore, alert.totalScore()),
        Math.max(lastAlertTimeMs, alert.eventTimeMs()),
        alert.alertId());
  }
}
