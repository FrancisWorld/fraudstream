package dev.francisworld.fraudstream.config;

import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

/** Builds client properties; pass-through overrides carry security settings such as SASL. */
public final class KafkaProperties {

  private KafkaProperties() {}

  public static Properties streams(AppConfig c) {
    Properties p = new Properties();
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, c.applicationId());
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, c.bootstrapServers());
    p.put(StreamsConfig.STATE_DIR_CONFIG, c.stateDir());
    p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, c.streamThreads());
    p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, (int) c.replicationFactor());
    p.put(StreamsConfig.APPLICATION_SERVER_CONFIG, c.advertisedHost() + ":" + c.httpPort());
    p.putAll(c.kafkaOverrides());
    return p;
  }

  public static Properties producer(AppConfig c) {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, c.bootstrapServers());
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    p.put(ProducerConfig.ACKS_CONFIG, "all");
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
    p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
    p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
    p.put(ProducerConfig.CLIENT_ID_CONFIG, c.applicationId() + "-api");
    p.putAll(c.kafkaOverrides());
    return p;
  }

  public static Properties admin(AppConfig c) {
    Properties p = new Properties();
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, c.bootstrapServers());
    p.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
    p.putAll(c.kafkaOverrides());
    return p;
  }
}
