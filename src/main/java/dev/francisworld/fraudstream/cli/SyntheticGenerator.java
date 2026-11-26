package dev.francisworld.fraudstream.cli;

import dev.francisworld.fraudstream.model.Transaction;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Deterministic generator of synthetic card activity with injected fraud episodes.
 *
 * <p>The data is invented. It contains legitimate behaviour that is deliberately hard for the
 * shipped rules (shopping sprees, large one-off purchases) as well as a fraud pattern no rule
 * targets, so precision and recall measured against it are neither trivially 1.0 nor meaningful as
 * a statement about real fraud.
 */
public final class SyntheticGenerator {

  public static final String VELOCITY = "velocity_burst";
  public static final String TRAVEL = "impossible_travel";
  public static final String MERCHANT = "merchant_anomaly";
  public static final String DEVICE_RING = "device_ring";
  public static final String LOW_AND_SLOW = "low_and_slow";

  private static final double[][] HOME_CITIES = {
    {-23.55, -46.63}, {-22.91, -43.17}, {-19.92, -43.94}, {-25.43, -49.27},
    {-30.03, -51.22}, {-12.97, -38.51}, {-8.05, -34.88}, {-15.79, -47.88}
  };
  private static final double[][] FAR_CITIES = {
    {6.52, 3.38}, {55.75, 37.62}, {21.03, 105.85}, {25.76, -80.19}
  };
  private static final String[] FAR_COUNTRIES = {"NG", "RU", "VN", "US"};
  private static final int MERCHANTS = 40;

  private record Merchant(String id, String category, double logMean) {}

  private record Account(String id, double[] home, List<Merchant> favourites, String device) {}

  private final int accountCount;
  private final int days;
  private final Instant start;
  private final SplittableRandom rnd;
  private final List<Merchant> merchants = new ArrayList<>();
  private final List<Labelled> out = new ArrayList<>();
  private int episodes;

  public SyntheticGenerator(int accounts, int days, long seed, Instant start) {
    if (accounts < 10 || days < 2) {
      throw new IllegalArgumentException("Need at least 10 accounts and 2 days");
    }
    this.accountCount = accounts;
    this.days = days;
    this.start = start;
    this.rnd = new SplittableRandom(seed);
  }

  public List<Labelled> generate() {
    for (int i = 1; i <= MERCHANTS; i++) {
      merchants.add(
          new Merchant(
              String.format("m-%03d", i),
              String.valueOf(5000 + (i % 8) * 100),
              Math.log(15) + rnd.nextDouble() * (Math.log(250) - Math.log(15))));
    }
    List<Account> accounts = new ArrayList<>();
    for (int i = 1; i <= accountCount; i++) {
      List<Merchant> favourites = new ArrayList<>();
      for (int k = 0; k < 4; k++) {
        favourites.add(merchants.get(rnd.nextInt(MERCHANTS)));
      }
      accounts.add(
          new Account(
              String.format("acct-%04d", i),
              HOME_CITIES[rnd.nextInt(HOME_CITIES.length)],
              favourites,
              String.format("dev-%04d", i)));
    }
    for (Account account : accounts) {
      for (int day = 0; day < days; day++) {
        legitimateDay(account, day);
      }
    }
    injectFraud(accounts);
    out.sort(
        Comparator.comparingLong((Labelled l) -> l.transaction().eventTimeMs())
            .thenComparing(l -> l.transaction().accountId())
            .thenComparing(l -> l.transaction().transactionId()));
    List<Labelled> numbered = new ArrayList<>(out.size());
    for (int i = 0; i < out.size(); i++) {
      Labelled l = out.get(i);
      Transaction t = l.transaction();
      numbered.add(
          new Labelled(
              new Transaction(
                  String.format("tx-%07d", i + 1),
                  t.accountId(),
                  t.amountMinor(),
                  t.currency(),
                  t.merchantId(),
                  t.merchantCategory(),
                  t.country(),
                  t.latitude(),
                  t.longitude(),
                  t.eventTimeMs(),
                  t.deviceId(),
                  t.ipAddress(),
                  t.channel()),
              l.label()));
    }
    return numbered;
  }

  private long at(int day, int utcHour, int minute, int second) {
    return start
        .plus(Duration.ofDays(day))
        .plusSeconds(utcHour * 3600L + minute * 60L + second)
        .toEpochMilli();
  }

  private void legitimateDay(Account a, int day) {
    int roll = rnd.nextInt(100);
    int count = roll < 30 ? 0 : roll < 60 ? 1 : roll < 85 ? 2 : roll < 95 ? 3 : 4;
    boolean tripDay = rnd.nextInt(100) < 1;
    for (int i = 0; i < count; i++) {
      Merchant m =
          rnd.nextInt(10) < 8
              ? a.favourites().get(rnd.nextInt(4))
              : merchants.get(rnd.nextInt(MERCHANTS));
      double mult = rnd.nextInt(1000) < 4 ? 8 + rnd.nextDouble() * 7 : 1.0;
      double[] where = tripDay ? HOME_CITIES[(day + i) % HOME_CITIES.length] : a.home();
      add(
          a,
          m,
          amount(m, mult),
          at(day, 11 + rnd.nextInt(13), rnd.nextInt(60), rnd.nextInt(60)),
          where,
          Labelled.Label.legitimate(),
          true);
    }
    if (rnd.nextInt(100) < 2) {
      Merchant m = merchants.get(rnd.nextInt(MERCHANTS));
      int size = 3 + rnd.nextInt(4);
      int hour = 14 + rnd.nextInt(6);
      for (int i = 0; i < size; i++) {
        add(
            a,
            m,
            amount(m, 0.6),
            at(day, hour, rnd.nextInt(2), i * 35 + rnd.nextInt(20)),
            a.home(),
            Labelled.Label.legitimate(),
            true);
      }
    }
  }

  private long amount(Merchant m, double multiplier) {
    double gaussian =
        Math.sqrt(-2 * Math.log(1 - rnd.nextDouble())) * Math.cos(2 * Math.PI * rnd.nextDouble());
    return Math.max(100, Math.round(Math.exp(m.logMean() + 0.35 * gaussian) * multiplier * 100));
  }

  private void add(
      Account a,
      Merchant m,
      long amountMinor,
      long ts,
      double[] where,
      Labelled.Label label,
      boolean withDevice) {
    boolean device = withDevice && rnd.nextInt(10) < 6;
    out.add(
        new Labelled(
            new Transaction(
                "pending",
                a.id(),
                amountMinor,
                "BRL",
                m.id(),
                m.category(),
                "BR",
                where[0] + (rnd.nextDouble() - 0.5) * 0.06,
                where[1] + (rnd.nextDouble() - 0.5) * 0.06,
                ts,
                device ? a.device() : null,
                device ? "198.51.100." + (1 + rnd.nextInt(250)) : null,
                device ? "mobile" : "pos"),
            label));
  }

  private void injectFraud(List<Account> accounts) {
    for (Account a : accounts) {
      if (rnd.nextInt(100) >= 4) {
        continue;
      }
      switch (rnd.nextInt(4)) {
        case 0 -> velocity(a);
        case 1 -> travel(a);
        case 2 -> merchantAnomaly(a);
        default -> lowAndSlow(a);
      }
    }
    int rings = Math.max(1, accountCount / 100);
    for (int r = 0; r < rings; r++) {
      ring(accounts, r);
    }
  }

  private String episode(String pattern) {
    return pattern + "-" + (++episodes);
  }

  private void velocity(Account a) {
    String ep = episode(VELOCITY);
    Merchant m = merchants.get(rnd.nextInt(MERCHANTS));
    int day = rnd.nextInt(days);
    int count = 7 + rnd.nextInt(4);
    long t = at(day, 15 + rnd.nextInt(6), rnd.nextInt(50), 0);
    for (int i = 0; i < count; i++) {
      long amount = 2000 + rnd.nextInt(7000);
      out.add(
          new Labelled(
              new Transaction(
                  "pending",
                  a.id(),
                  amount,
                  "BRL",
                  m.id(),
                  m.category(),
                  "BR",
                  null,
                  null,
                  t,
                  null,
                  null,
                  "web"),
              new Labelled.Label(true, VELOCITY, ep)));
      t += (20 + rnd.nextInt(40)) * 1000L;
    }
  }

  private void travel(Account a) {
    String ep = episode(TRAVEL);
    int day = rnd.nextInt(days);
    long t = at(day, 13 + rnd.nextInt(6), rnd.nextInt(40), 0);
    Merchant home = a.favourites().get(0);
    add(a, home, amount(home, 1.0), t, a.home(), Labelled.Label.legitimate(), true);
    int far = rnd.nextInt(FAR_CITIES.length);
    Merchant m = merchants.get(rnd.nextInt(MERCHANTS));
    long ts = t + (5 + rnd.nextInt(16)) * 60_000L;
    out.add(
        new Labelled(
            new Transaction(
                "pending",
                a.id(),
                10_000 + rnd.nextInt(80_000),
                "BRL",
                m.id(),
                m.category(),
                FAR_COUNTRIES[far],
                FAR_CITIES[far][0],
                FAR_CITIES[far][1],
                ts,
                null,
                null,
                "pos"),
            new Labelled.Label(true, TRAVEL, ep)));
  }

  private void merchantAnomaly(Account a) {
    String ep = episode(MERCHANT);
    Merchant m = a.favourites().get(rnd.nextInt(4));
    int day = days / 2 + rnd.nextInt(days - days / 2);
    long ts = at(day, 14 + rnd.nextInt(6), rnd.nextInt(60), 0);
    out.add(
        new Labelled(
            new Transaction(
                "pending",
                a.id(),
                amount(m, 40 + rnd.nextInt(40)),
                "BRL",
                m.id(),
                m.category(),
                "BR",
                a.home()[0],
                a.home()[1],
                ts,
                null,
                null,
                "web"),
            new Labelled.Label(true, MERCHANT, ep)));
  }

  private void lowAndSlow(Account a) {
    String ep = episode(LOW_AND_SLOW);
    int day = rnd.nextInt(days);
    Merchant m = merchants.get(rnd.nextInt(MERCHANTS));
    long t = at(day, 14, rnd.nextInt(30), 0);
    for (int i = 0; i < 3; i++) {
      out.add(
          new Labelled(
              new Transaction(
                  "pending",
                  a.id(),
                  25_000 + rnd.nextInt(15_000),
                  "BRL",
                  m.id(),
                  m.category(),
                  "BR",
                  a.home()[0],
                  a.home()[1],
                  t,
                  null,
                  null,
                  "web"),
              new Labelled.Label(true, LOW_AND_SLOW, ep)));
      t += 40 * 60_000L;
    }
  }

  private void ring(List<Account> accounts, int index) {
    String ep = episode(DEVICE_RING);
    int size = 4 + rnd.nextInt(3);
    int day = rnd.nextInt(days);
    long t = at(day, 16, rnd.nextInt(10), 0);
    String device = "dev-ring-" + (index + 1);
    for (int i = 0; i < size; i++) {
      Account victim = accounts.get(rnd.nextInt(accounts.size()));
      Merchant m = merchants.get(rnd.nextInt(MERCHANTS));
      out.add(
          new Labelled(
              new Transaction(
                  "pending",
                  victim.id(),
                  8_000 + rnd.nextInt(22_000),
                  "BRL",
                  m.id(),
                  m.category(),
                  "BR",
                  null,
                  null,
                  t,
                  device,
                  "203.0.113.50",
                  "web"),
              new Labelled.Label(true, DEVICE_RING, ep)));
      t += (3 + rnd.nextInt(8)) * 60_000L;
    }
  }
}
