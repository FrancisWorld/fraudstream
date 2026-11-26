package dev.francisworld.fraudstream.api;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Authenticates instance-to-instance forwarding. The hop header carries {@code <epoch-ms>.<hmac>}
 * where the HMAC-SHA256 covers method, path, query and timestamp under a secret shared by all
 * instances. A client that merely sends the header cannot pass for a peer: without a valid
 * signature the header is ignored.
 */
public final class HopAuth {

  public static final String HEADER = "x-fraudstream-hop";
  static final int MIN_SECRET_LENGTH = 16;
  private static final Duration MAX_SKEW = Duration.ofSeconds(60);

  private final byte[] secret;
  private final Clock clock;

  public HopAuth(String secret, Clock clock) {
    if (secret == null || secret.length() < MIN_SECRET_LENGTH) {
      throw new IllegalArgumentException(
          "Peer secret must be at least " + MIN_SECRET_LENGTH + " characters");
    }
    this.secret = secret.getBytes(StandardCharsets.UTF_8);
    this.clock = clock;
  }

  /** A random secret: peers cannot share it, so forwarding works within one instance only. */
  public static HopAuth random() {
    byte[] bytes = new byte[32];
    new SecureRandom().nextBytes(bytes);
    return new HopAuth(HexFormat.of().formatHex(bytes), Clock.systemUTC());
  }

  public String sign(String method, String path, String rawQuery) {
    long now = clock.millis();
    return now + "." + mac(method, path, rawQuery, now);
  }

  public boolean verify(String header, String method, String path, String rawQuery) {
    if (header == null) {
      return false;
    }
    int dot = header.indexOf('.');
    if (dot <= 0) {
      return false;
    }
    long timestamp;
    try {
      timestamp = Long.parseLong(header.substring(0, dot));
    } catch (NumberFormatException e) {
      return false;
    }
    if (Math.abs(clock.millis() - timestamp) > MAX_SKEW.toMillis()) {
      return false;
    }
    byte[] expected = mac(method, path, rawQuery, timestamp).getBytes(StandardCharsets.UTF_8);
    byte[] presented = header.substring(dot + 1).getBytes(StandardCharsets.UTF_8);
    return MessageDigest.isEqual(expected, presented);
  }

  private String mac(String method, String path, String rawQuery, long timestamp) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret, "HmacSHA256"));
      String message =
          method + "\n" + path + "\n" + (rawQuery == null ? "" : rawQuery) + "\n" + timestamp;
      return HexFormat.of().formatHex(mac.doFinal(message.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new IllegalStateException(e);
    }
  }
}
