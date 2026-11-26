package dev.francisworld.fraudstream.config;

import dev.francisworld.fraudstream.topology.FraudSettings;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.TreeMap;

/** Runtime configuration, read from environment variables so containers need no config files. */
public record AppConfig(
    String bootstrapServers,
    String applicationId,
    String stateDir,
    String topicPrefix,
    int streamThreads,
    int partitions,
    short replicationFactor,
    boolean createTopics,
    boolean seedRules,
    int httpPort,
    int managementPort,
    String advertisedHost,
    String authTokens,
    String peerSecret,
    FraudSettings settings,
    Map<String, String> kafkaOverrides) {

  private static final String KAFKA_PREFIX = "FRAUDSTREAM_KAFKA_";

  /**
   * @throws IllegalArgumentException with a message naming the offending variable
   */
  public static AppConfig fromEnv(Map<String, String> env) {
    int httpPort = integer(env, "FRAUDSTREAM_HTTP_PORT", 8080, 1, 65_535);
    Map<String, String> overrides = new TreeMap<>();
    env.forEach((k, v) -> {
      if (k.startsWith(KAFKA_PREFIX) && k.length() > KAFKA_PREFIX.length()) {
        overrides.put(
            k.substring(KAFKA_PREFIX.length()).toLowerCase(java.util.Locale.ROOT).replace('_', '.'),
            v);
      }
    });
    FraudSettings settings =
        new FraudSettings(
            duration(env, "FRAUDSTREAM_GRACE", Duration.ofMinutes(5)),
            duration(env, "FRAUDSTREAM_DEVICE_WINDOW", Duration.ofHours(24)),
            env.getOrDefault("FRAUDSTREAM_ACTIVE_RULE_SET", "default"));
    return new AppConfig(
        env.getOrDefault("FRAUDSTREAM_BOOTSTRAP_SERVERS", "localhost:9092"),
        env.getOrDefault("FRAUDSTREAM_APPLICATION_ID", "fraudstream"),
        env.getOrDefault("FRAUDSTREAM_STATE_DIR", "./state"),
        env.getOrDefault("FRAUDSTREAM_TOPIC_PREFIX", "fraud"),
        integer(env, "FRAUDSTREAM_STREAM_THREADS", 1, 1, 64),
        integer(env, "FRAUDSTREAM_PARTITIONS", 3, 1, 1_000),
        (short) integer(env, "FRAUDSTREAM_REPLICATION_FACTOR", 1, 1, 7),
        bool(env, "FRAUDSTREAM_CREATE_TOPICS", true),
        bool(env, "FRAUDSTREAM_SEED_RULES", false),
        httpPort,
        integer(env, "FRAUDSTREAM_MANAGEMENT_PORT", 8081, 1, 65_535),
        advertisedHost(env.getOrDefault("FRAUDSTREAM_ADVERTISED_HOST", "localhost")),
        env.get("FRAUDSTREAM_AUTH_TOKENS"),
        env.get("FRAUDSTREAM_PEER_SECRET"),
        settings,
        Map.copyOf(overrides));
  }

  /** "auto" resolves the container's own address, which is what peers must dial on Fargate. */
  private static String advertisedHost(String configured) {
    if (!configured.equals("auto")) {
      return configured;
    }
    try {
      return java.net.InetAddress.getLocalHost().getHostAddress();
    } catch (java.net.UnknownHostException e) {
      throw new IllegalArgumentException(
          "FRAUDSTREAM_ADVERTISED_HOST=auto but the local address cannot be resolved");
    }
  }

  private static int integer(Map<String, String> env, String name, int fallback, int min, int max) {
    String raw = env.get(name);
    if (raw == null || raw.isBlank()) {
      return fallback;
    }
    try {
      int value = Integer.parseInt(raw.trim());
      if (value < min || value > max) {
        throw new IllegalArgumentException(name + " must be between " + min + " and " + max);
      }
      return value;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be an integer");
    }
  }

  private static boolean bool(Map<String, String> env, String name, boolean fallback) {
    String raw = env.get(name);
    if (raw == null || raw.isBlank()) {
      return fallback;
    }
    if (!raw.equalsIgnoreCase("true") && !raw.equalsIgnoreCase("false")) {
      throw new IllegalArgumentException(name + " must be true or false");
    }
    return Boolean.parseBoolean(raw);
  }

  private static Duration duration(Map<String, String> env, String name, Duration fallback) {
    String raw = env.get(name);
    if (raw == null || raw.isBlank()) {
      return fallback;
    }
    try {
      return Duration.parse(raw.trim());
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(name + " must be an ISO-8601 duration such as PT5M");
    }
  }
}
