package dev.francisworld.fraudstream.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.francisworld.fraudstream.topology.Topics;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

class AppConfigTest {

  @Test
  void defaultsAreSensible() {
    AppConfig c = AppConfig.fromEnv(Map.of());
    assertThat(c.bootstrapServers()).isEqualTo("localhost:9092");
    assertThat(c.httpPort()).isEqualTo(8080);
    assertThat(c.settings().grace()).isEqualTo(Duration.ofMinutes(5));
    assertThat(c.createTopics()).isTrue();
    assertThat(c.seedRules()).isFalse();
    assertThat(c.authTokens()).isNull();
  }

  @Test
  void readsOverridesAndPassesKafkaSettingsThrough() {
    AppConfig c =
        AppConfig.fromEnv(
            Map.of(
                "FRAUDSTREAM_HTTP_PORT", "9000",
                "FRAUDSTREAM_GRACE", "PT30S",
                "FRAUDSTREAM_SEED_RULES", "true",
                "FRAUDSTREAM_KAFKA_SECURITY_PROTOCOL", "SASL_SSL",
                "FRAUDSTREAM_KAFKA_SASL_MECHANISM", "AWS_MSK_IAM",
                "FRAUDSTREAM_KAFKA_", "ignored"));
    assertThat(c.httpPort()).isEqualTo(9000);
    assertThat(c.settings().grace()).isEqualTo(Duration.ofSeconds(30));
    assertThat(c.seedRules()).isTrue();
    assertThat(c.kafkaOverrides())
        .containsOnly(
            Map.entry("security.protocol", "SASL_SSL"), Map.entry("sasl.mechanism", "AWS_MSK_IAM"));
  }

  @Test
  void invalidValuesNameTheVariable() {
    assertThatThrownBy(() -> AppConfig.fromEnv(Map.of("FRAUDSTREAM_HTTP_PORT", "abc")))
        .hasMessageContaining("FRAUDSTREAM_HTTP_PORT");
    assertThatThrownBy(() -> AppConfig.fromEnv(Map.of("FRAUDSTREAM_HTTP_PORT", "70000")))
        .hasMessageContaining("between");
    assertThatThrownBy(() -> AppConfig.fromEnv(Map.of("FRAUDSTREAM_GRACE", "5 minutes")))
        .hasMessageContaining("ISO-8601");
    assertThatThrownBy(() -> AppConfig.fromEnv(Map.of("FRAUDSTREAM_SEED_RULES", "yes")))
        .hasMessageContaining("true or false");
    assertThatThrownBy(() -> AppConfig.fromEnv(Map.of("FRAUDSTREAM_GRACE", "PT-5M")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void streamsPropertiesEnableExactlyOnceV2AndAdvertiseTheEndpoint() {
    AppConfig c =
        AppConfig.fromEnv(
            Map.of(
                "FRAUDSTREAM_ADVERTISED_HOST",
                "node-a",
                "FRAUDSTREAM_KAFKA_SECURITY_PROTOCOL",
                "SSL"));
    Properties p = KafkaProperties.streams(c);
    assertThat(p.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))
        .isEqualTo(StreamsConfig.EXACTLY_ONCE_V2);
    assertThat(p.get(StreamsConfig.APPLICATION_SERVER_CONFIG)).isEqualTo("node-a:8080");
    assertThat(p.get("security.protocol")).isEqualTo("SSL");
    assertThat(KafkaProperties.producer(c).get("enable.idempotence")).isEqualTo(true);
    assertThat(KafkaProperties.producer(c).get("acks")).isEqualTo("all");
    assertThat(KafkaProperties.admin(c).get("security.protocol")).isEqualTo("SSL");
  }

  @Test
  void topicPlanKeepsCoPartitionedTopicsAlignedAndCompactsRules() {
    var plan = TopicProvisioner.plan(Topics.defaults(), 6, (short) 3);
    assertThat(plan).hasSize(7);
    var byName = plan.stream().collect(java.util.stream.Collectors.toMap(t -> t.name(), t -> t));
    assertThat(byName.get("fraud.transactions").numPartitions()).isEqualTo(6);
    assertThat(byName.get("fraud.case-commands").numPartitions()).isEqualTo(6);
    assertThat(byName.get("fraud.rules").numPartitions()).isEqualTo(1);
    assertThat(byName.get("fraud.rules").configs()).containsEntry("cleanup.policy", "compact");
    assertThat(byName.get("fraud.alerts").replicationFactor()).isEqualTo((short) 3);
  }
}
