package dev.francisworld.fraudstream.api;

import dev.francisworld.fraudstream.model.RuleSetDocument;
import dev.francisworld.fraudstream.query.StateAccess;
import dev.francisworld.fraudstream.rules.RuleCompiler;
import dev.francisworld.fraudstream.rules.RuleSetInvalidException;
import dev.francisworld.fraudstream.serde.Json;
import dev.francisworld.fraudstream.topology.StoreNames;
import dev.francisworld.fraudstream.topology.Topics;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/** Publish, validate and read the versioned rule sets that every instance hot-reloads. */
public final class RuleRoutes {

  private final Producer<String, byte[]> producer;
  private final Topics topics;
  private final StateAccess state;
  private final String activeRuleSet;

  public RuleRoutes(
      Producer<String, byte[]> producer, Topics topics, StateAccess state, String activeRuleSet) {
    this.producer = producer;
    this.topics = topics;
    this.state = state;
    this.activeRuleSet = activeRuleSet;
  }

  public void register(Router router) {
    router.route("PUT", "/api/v1/rules", Permission.MANAGE_RULES, this::publish);
    router.route("POST", "/api/v1/rules/validate", Permission.MANAGE_RULES, this::validate);
    router.route("GET", "/api/v1/rules", Permission.READ, this::active);
  }

  private static RuleSetDocument parse(Request request) {
    try {
      return Json.MAPPER.readValue(request.body(), RuleSetDocument.class);
    } catch (IOException e) {
      throw ApiException.badRequest("Body is not a valid rule set document", java.util.List.of());
    }
  }

  private static void compile(RuleSetDocument doc) {
    try {
      RuleCompiler.compile(doc);
    } catch (RuleSetInvalidException e) {
      throw new ApiException(422, "invalid_rule_set", "Rule set failed validation", e.errors());
    }
  }

  private Response validate(Request request) {
    compile(parse(request));
    return Response.json(200, Map.of("valid", true));
  }

  private Response publish(Request request) {
    RuleSetDocument doc = parse(request);
    compile(doc);
    byte[] current = this.<byte[]>rules().get(doc.name());
    if (current != null) {
      long currentVersion = Json.read(current, RuleSetDocument.class).version();
      if (doc.version() <= currentVersion) {
        throw new ApiException(
            409,
            "version_conflict",
            "Active version is " + currentVersion + "; publish a higher one");
      }
    }
    try {
      producer
          .send(new ProducerRecord<>(topics.rules(), doc.name(), Json.write(doc)))
          .get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw ApiException.unavailable("Interrupted while publishing");
    } catch (ExecutionException | TimeoutException e) {
      throw ApiException.unavailable("Could not publish to Kafka, retry later");
    }
    return Response.json(202, Map.of("name", doc.name(), "version", doc.version()));
  }

  private Response active(Request request) {
    String name = request.query().getOrDefault("name", activeRuleSet);
    byte[] current = this.<byte[]>rules().get(name);
    if (current == null) {
      throw ApiException.notFound("No rule set named '" + name + "' has been published");
    }
    return new Response(200, current, "application/json", Map.of());
  }

  private <V> ReadOnlyKeyValueStore<String, V> rules() {
    return state.store(StoreNames.RULES);
  }
}
