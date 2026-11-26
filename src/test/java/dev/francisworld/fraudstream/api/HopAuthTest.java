package dev.francisworld.fraudstream.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class HopAuthTest {

  private static final Instant NOW = Instant.parse("2026-03-02T12:00:00Z");
  private static final String SECRET = "a-shared-secret-123456";

  private static HopAuth at(Instant instant, String secret) {
    return new HopAuth(secret, Clock.fixed(instant, ZoneOffset.UTC));
  }

  @Test
  void aFreshSignatureVerifiesForTheSameRequestOnly() {
    String header = at(NOW, SECRET).sign("GET", "/api/v1/cases", "status=OPEN");
    HopAuth verifier = at(NOW.plusSeconds(5), SECRET);
    assertThat(verifier.verify(header, "GET", "/api/v1/cases", "status=OPEN")).isTrue();
    assertThat(verifier.verify(header, "POST", "/api/v1/cases", "status=OPEN")).isFalse();
    assertThat(verifier.verify(header, "GET", "/api/v1/cases/x", "status=OPEN")).isFalse();
    assertThat(verifier.verify(header, "GET", "/api/v1/cases", "status=CLOSED")).isFalse();
    assertThat(verifier.verify(header, "GET", "/api/v1/cases", null)).isFalse();
  }

  @Test
  void signaturesExpireAndRejectOtherSecrets() {
    String header = at(NOW, SECRET).sign("GET", "/p", null);
    assertThat(at(NOW.plus(Duration.ofSeconds(61)), SECRET).verify(header, "GET", "/p", null))
        .isFalse();
    assertThat(at(NOW.minus(Duration.ofSeconds(61)), SECRET).verify(header, "GET", "/p", null))
        .isFalse();
    assertThat(at(NOW, "another-secret-1234567").verify(header, "GET", "/p", null)).isFalse();
  }

  @Test
  void malformedHeadersAreRejectedNotCrashes() {
    HopAuth auth = at(NOW, SECRET);
    for (String bad :
        new String[] {
          null, "", "1", ".abc", "abc.def", NOW.toEpochMilli() + ".", NOW.toEpochMilli() + ".zz"
        }) {
      assertThat(auth.verify(bad, "GET", "/p", null)).as(String.valueOf(bad)).isFalse();
    }
  }

  @Test
  void weakSecretsAreRefusedAndRandomSecretsDiffer() {
    assertThatThrownBy(() -> new HopAuth("short", Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new HopAuth(null, Clock.systemUTC()))
        .isInstanceOf(IllegalArgumentException.class);
    String header = HopAuth.random().sign("GET", "/p", null);
    assertThat(HopAuth.random().verify(header, "GET", "/p", null)).isFalse();
  }
}
