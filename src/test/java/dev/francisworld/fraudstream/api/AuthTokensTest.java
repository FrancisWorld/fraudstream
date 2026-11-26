package dev.francisworld.fraudstream.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class AuthTokensTest {

  private static final String SPEC =
      "alice:analyst:"
          + AuthTokens.hash("alice-secret")
          + ", ingest-1:INGEST:"
          + AuthTokens.hash("pipe");

  @Test
  void matchesBearerTokensCaseInsensitivelyOnTheScheme() {
    AuthTokens tokens = AuthTokens.parse(SPEC);
    assertThat(tokens.authenticate("Bearer alice-secret"))
        .contains(new Principal("alice", Role.ANALYST));
    assertThat(tokens.authenticate("bearer pipe")).contains(new Principal("ingest-1", Role.INGEST));
  }

  @Test
  void rejectsUnknownMissingOrNonBearerCredentials() {
    AuthTokens tokens = AuthTokens.parse(SPEC);
    assertThat(tokens.authenticate(null)).isEmpty();
    assertThat(tokens.authenticate("Bearer ")).isEmpty();
    assertThat(tokens.authenticate("Bearer nope")).isEmpty();
    assertThat(tokens.authenticate("Basic YWxpY2U6")).isEmpty();
    assertThat(tokens.authenticate("alice-secret")).isEmpty();
    assertThat(tokens.authenticate("Bearer " + AuthTokens.hash("alice-secret"))).isEmpty();
  }

  @Test
  void refusesToRunWithoutOrWithMalformedConfiguration() {
    assertThatThrownBy(() -> AuthTokens.parse(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> AuthTokens.parse(" ")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> AuthTokens.parse("a:ADMIN")).hasMessageContaining("name:ROLE");
    assertThatThrownBy(() -> AuthTokens.parse("a:GOD:" + AuthTokens.hash("x")))
        .hasMessageContaining("Unknown role");
    assertThatThrownBy(() -> AuthTokens.parse("a:ADMIN:plaintext"))
        .hasMessageContaining("64 lowercase hex");
    assertThatThrownBy(() -> AuthTokens.parse("bad name:ADMIN:" + AuthTokens.hash("x")))
        .isInstanceOf(IllegalArgumentException.class);
    String h = AuthTokens.hash("x");
    assertThatThrownBy(() -> AuthTokens.parse("a:ADMIN:" + h + ",a:INGEST:" + h))
        .hasMessageContaining("Duplicate");
  }

  @Test
  void rolesGrantOnlyTheirPermissions() {
    assertThat(Role.INGEST.allows(Permission.INGEST_TRANSACTIONS)).isTrue();
    assertThat(Role.INGEST.allows(Permission.READ)).isFalse();
    assertThat(Role.ANALYST.allows(Permission.WORK_CASES)).isTrue();
    assertThat(Role.ANALYST.allows(Permission.MANAGE_RULES)).isFalse();
    assertThat(Role.ANALYST.allows(Permission.INGEST_TRANSACTIONS)).isFalse();
    for (Permission p : Permission.values()) {
      assertThat(Role.ADMIN.allows(p)).isTrue();
    }
  }
}
