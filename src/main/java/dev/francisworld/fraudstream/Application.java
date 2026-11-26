package dev.francisworld.fraudstream;

import dev.francisworld.fraudstream.api.AccountRoutes;
import dev.francisworld.fraudstream.api.AuthTokens;
import dev.francisworld.fraudstream.api.CaseRoutes;
import dev.francisworld.fraudstream.api.Forwarder;
import dev.francisworld.fraudstream.api.HttpService;
import dev.francisworld.fraudstream.api.ManagementRoutes;
import dev.francisworld.fraudstream.api.Router;
import dev.francisworld.fraudstream.api.RuleRoutes;
import dev.francisworld.fraudstream.api.TransactionRoutes;
import dev.francisworld.fraudstream.config.AppConfig;
import dev.francisworld.fraudstream.config.KafkaProperties;
import dev.francisworld.fraudstream.config.TopicProvisioner;
import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.query.StreamsStateAccess;
import dev.francisworld.fraudstream.schema.SchemaRegistry;
import dev.francisworld.fraudstream.schema.TransactionCodec;
import dev.francisworld.fraudstream.schema.TransactionRequestValidator;
import dev.francisworld.fraudstream.serde.Json;
import dev.francisworld.fraudstream.topology.FraudMetrics;
import dev.francisworld.fraudstream.topology.FraudTopology;
import dev.francisworld.fraudstream.topology.StoreNames;
import dev.francisworld.fraudstream.topology.Topics;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Wires and owns the long-running pieces: Kafka Streams, the producer and both HTTP servers. */
public final class Application implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Application.class);

  private final KafkaStreams streams;
  private final KafkaProducer<String, byte[]> producer;
  private final HttpService api;
  private final HttpService management;
  private final KafkaClientMetrics producerMetrics;
  private final AtomicReference<KafkaStreams.State> state =
      new AtomicReference<>(KafkaStreams.State.CREATED);

  private Application(AppConfig config) throws IOException, InterruptedException {
    Topics topics = Topics.withPrefix(config.topicPrefix());
    PrometheusMeterRegistry meters = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    TransactionCodec codec = new TransactionCodec(SchemaRegistry.load());
    AuthTokens auth = AuthTokens.parse(config.authTokens());

    if (config.createTopics()) {
      try (Admin admin = Admin.create(KafkaProperties.admin(config))) {
        TopicProvisioner.ensure(
            admin, TopicProvisioner.plan(topics, config.partitions(), config.replicationFactor()));
      }
    }

    var topology =
        new FraudTopology(topics, config.settings(), new FraudMetrics(meters), codec).build();
    streams = new KafkaStreams(topology, KafkaProperties.streams(config));
    streams.setUncaughtExceptionHandler(e -> {
      LOG.error("Uncaught stream thread failure, replacing thread", e);
      return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
    });
    streams.setStateListener((now, before) -> state.set(now));
    new io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics(streams).bindTo(meters);

    producer = new KafkaProducer<>(KafkaProperties.producer(config));
    producerMetrics = new KafkaClientMetrics(producer);
    producerMetrics.bindTo(meters);

    HostInfo self = new HostInfo(config.advertisedHost(), config.httpPort());
    var access = new StreamsStateAccess(streams, self);
    var forwarder = new Forwarder();
    Router router = new Router(auth, access, forwarder);
    new TransactionRoutes(producer, topics, new TransactionRequestValidator(), codec)
        .register(router);
    new RuleRoutes(producer, topics, access, config.settings().activeRuleSet()).register(router);
    new AccountRoutes(access).register(router);
    new CaseRoutes(producer, topics, access, forwarder, Clock.systemUTC()).register(router);

    streams.start();
    api = new HttpService(config.httpPort(), router::dispatch);
    management =
        new HttpService(
            config.managementPort(),
            ManagementRoutes.create(meters, () -> state.get() == KafkaStreams.State.RUNNING)
                ::dispatch);
    api.start();
    management.start();
    if (config.seedRules()) {
      seedRules(topics, config.settings().activeRuleSet(), access);
    }
  }

  public static Application start(AppConfig config) throws IOException, InterruptedException {
    return new Application(config);
  }

  /**
   * Publishes the bundled rule set only when none exists yet, so restarts never roll rules back.
   */
  private void seedRules(Topics topics, String name, StreamsStateAccess access)
      throws IOException, InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
    while (state.get() != KafkaStreams.State.RUNNING) {
      if (System.nanoTime() > deadline) {
        LOG.warn("Streams not running after 90s; skipping rule seeding");
        return;
      }
      Thread.sleep(200);
    }
    byte[] existing = access.<byte[]>store(StoreNames.RULES).get(name);
    if (existing != null) {
      LOG.info("Rule set '{}' already present, not seeding", name);
      return;
    }
    try (InputStream in = Application.class.getResourceAsStream("/rules/default.json")) {
      RuleSetDocument doc = Json.MAPPER.readValue(in, RuleSetDocument.class);
      producer.send(new ProducerRecord<>(topics.rules(), doc.name(), Json.write(doc)));
      producer.flush();
      LOG.info("Seeded rule set '{}' v{}", doc.name(), doc.version());
    }
  }

  @Override
  public void close() {
    LOG.info("Shutting down");
    api.close();
    management.close();
    streams.close(Duration.ofSeconds(30));
    producerMetrics.close();
    producer.close(Duration.ofSeconds(5));
  }
}
