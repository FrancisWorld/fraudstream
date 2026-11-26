package dev.francisworld.fraudstream.api;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Bearer-token authentication against a configured allow-list.
 *
 * <p>The spec is a comma separated list of {@code name:ROLE:sha256-hex}. Only hashes are
 * configured, so a leaked config file or environment dump does not reveal usable tokens, and every
 * comparison is constant time. The authenticated name, not anything in a request body, identifies
 * the actor in audit trails.
 */
public final class AuthTokens {

  private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_.@-]{1,64}");
  private static final Pattern HEX = Pattern.compile("[0-9a-f]{64}");

  private record Entry(Principal principal, byte[] hash) {}

  private final List<Entry> entries;

  private AuthTokens(List<Entry> entries) {
    this.entries = entries;
  }

  /**
   * @throws IllegalArgumentException when the spec is empty or malformed; the service refuses to
   *     start without authentication rather than running open
   */
  public static AuthTokens parse(String spec) {
    if (spec == null || spec.isBlank()) {
      throw new IllegalArgumentException("No API tokens configured (FRAUDSTREAM_AUTH_TOKENS)");
    }
    List<Entry> entries = new ArrayList<>();
    Set<String> names = new TreeSet<>();
    for (String raw : spec.split(",")) {
      String[] parts = raw.trim().split(":");
      if (parts.length != 3 || !NAME.matcher(parts[0]).matches()) {
        throw new IllegalArgumentException("Token entries look like name:ROLE:sha256hex");
      }
      Role role;
      try {
        role = Role.valueOf(parts[1].toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unknown role '" + parts[1] + "' for " + parts[0]);
      }
      if (!HEX.matcher(parts[2]).matches()) {
        throw new IllegalArgumentException(
            "Token hash for " + parts[0] + " must be 64 lowercase hex characters");
      }
      if (!names.add(parts[0])) {
        throw new IllegalArgumentException("Duplicate token name " + parts[0]);
      }
      entries.add(new Entry(new Principal(parts[0], role), HexFormat.of().parseHex(parts[2])));
    }
    return new AuthTokens(List.copyOf(entries));
  }

  /** Returns the caller for an {@code Authorization: Bearer <token>} header value. */
  public Optional<Principal> authenticate(String authorizationHeader) {
    if (authorizationHeader == null
        || !authorizationHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
      return Optional.empty();
    }
    byte[] presented = sha256(authorizationHeader.substring(7).trim());
    Principal match = null;
    // Always scan every entry so timing does not reveal which one matched.
    for (Entry entry : entries) {
      if (MessageDigest.isEqual(entry.hash(), presented)) {
        match = entry.principal();
      }
    }
    return Optional.ofNullable(match);
  }

  public static String hash(String token) {
    return HexFormat.of().formatHex(sha256(token));
  }

  private static byte[] sha256(String value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
