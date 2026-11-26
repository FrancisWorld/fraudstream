package dev.francisworld.fraudstream.api;

import dev.francisworld.fraudstream.cases.AccountCases;
import dev.francisworld.fraudstream.cases.CaseCommand;
import dev.francisworld.fraudstream.cases.CaseRecord;
import dev.francisworld.fraudstream.cases.CaseStateMachine;
import dev.francisworld.fraudstream.cases.CaseStatus;
import dev.francisworld.fraudstream.cases.CommandType;
import dev.francisworld.fraudstream.query.StateAccess;
import dev.francisworld.fraudstream.serde.Json;
import dev.francisworld.fraudstream.topology.StoreNames;
import dev.francisworld.fraudstream.topology.Topics;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/** Case management: list and read cases through interactive queries, act through commands. */
public final class CaseRoutes {

  /** Body of {@code POST /cases/{id}/actions}. */
  public record ActionRequest(CommandType type, String comment, String commandId) {}

  /** Response shape of the list endpoint; also what peers return during fan-out. */
  public record CaseList(List<CaseRecord> cases, boolean partial) {}

  private static final Pattern COMMAND_ID = Pattern.compile("[A-Za-z0-9_-]{8,64}");
  private static final int DEFAULT_LIMIT = 50;
  private static final int MAX_LIMIT = 200;

  private final Producer<String, byte[]> producer;
  private final Topics topics;
  private final StateAccess state;
  private final Forwarder forwarder;
  private final Clock clock;

  public CaseRoutes(
      Producer<String, byte[]> producer,
      Topics topics,
      StateAccess state,
      Forwarder forwarder,
      Clock clock) {
    this.producer = producer;
    this.topics = topics;
    this.state = state;
    this.forwarder = forwarder;
    this.clock = clock;
  }

  public void register(Router router) {
    router.route("GET", "/api/v1/cases", Permission.READ, this::list);
    router.route("GET", "/api/v1/cases/{id}", Permission.READ, CaseRoutes::owner, this::get);
    router.route(
        "POST", "/api/v1/cases/{id}/actions", Permission.WORK_CASES, CaseRoutes::owner, this::act);
  }

  private static Router.Owner owner(Request request) {
    String account = AccountCases.accountOf(request.pathParam("id"));
    if (account == null) {
      throw ApiException.notFound("Unknown case");
    }
    return new Router.Owner(StoreNames.CASES, account);
  }

  private ReadOnlyKeyValueStore<String, AccountCases> store() {
    return state.store(StoreNames.CASES);
  }

  private CaseRecord find(String caseId) {
    AccountCases cases = store().get(AccountCases.accountOf(caseId));
    return cases == null ? null : cases.find(caseId).orElse(null);
  }

  private Response get(Request request) {
    CaseRecord found = find(request.pathParam("id"));
    if (found == null) {
      throw ApiException.notFound("Unknown case");
    }
    return Response.json(200, found);
  }

  private Response list(Request request) {
    Map<String, String> q = request.query();
    CaseStatus status = null;
    if (q.containsKey("status")) {
      try {
        status = CaseStatus.valueOf(q.get("status"));
      } catch (IllegalArgumentException e) {
        throw ApiException.badRequest("Unknown status '" + q.get("status") + "'", List.of());
      }
    }
    int limit = DEFAULT_LIMIT;
    if (q.containsKey("limit")) {
      try {
        limit = Math.min(MAX_LIMIT, Math.max(1, Integer.parseInt(q.get("limit"))));
      } catch (NumberFormatException e) {
        throw ApiException.badRequest("limit must be an integer", List.of());
      }
    }
    List<CaseRecord> matches =
        new ArrayList<>(scanLocal(status, q.get("assignee"), q.get("accountId")));
    boolean partial = false;
    if (!request.isForwarded() && !"true".equals(q.get("local"))) {
      for (StateAccess.Peer peer : state.peers()) {
        try {
          String query =
              request.rawQuery() == null || request.rawQuery().isEmpty()
                  ? "local=true"
                  : request.rawQuery() + "&local=true";
          Response r =
              forwarder.get(
                  peer.host(),
                  peer.port(),
                  request.path(),
                  query,
                  request.header("authorization").orElse(null));
          if (r.status() != 200) {
            partial = true;
            continue;
          }
          matches.addAll(Json.MAPPER.readValue(r.body(), CaseList.class).cases());
        } catch (IOException | ApiException e) {
          partial = true;
        }
      }
    }
    matches.sort(
        Comparator.comparingLong(CaseRecord::updatedAtMs)
            .reversed()
            .thenComparing(CaseRecord::caseId));
    List<CaseRecord> page = matches.size() > limit ? matches.subList(0, limit) : matches;
    return Response.json(200, new CaseList(page, partial));
  }

  private List<CaseRecord> scanLocal(CaseStatus status, String assignee, String accountId) {
    List<CaseRecord> out = new ArrayList<>();
    try (KeyValueIterator<String, AccountCases> it = store().all()) {
      while (it.hasNext()) {
        KeyValue<String, AccountCases> kv = it.next();
        if (accountId != null && !accountId.equals(kv.key)) {
          continue;
        }
        for (CaseRecord c : kv.value.cases()) {
          if ((status == null || c.status() == status)
              && (assignee == null || assignee.equals(c.assignee()))) {
            out.add(c);
          }
        }
      }
    }
    return out;
  }

  private Response act(Request request) {
    String caseId = request.pathParam("id");
    ActionRequest body;
    try {
      body = Json.MAPPER.readValue(request.body(), ActionRequest.class);
    } catch (IOException e) {
      throw ApiException.badRequest(
          "Body must be {\"type\": CLAIM|RELEASE|CONFIRM_FRAUD|DISMISS|COMMENT, \"comment\": ...}",
          List.of());
    }
    if (body.type() == null) {
      throw ApiException.badRequest("type is required", List.of());
    }
    String commandId =
        body.commandId() != null
            ? body.commandId()
            : request.header("idempotency-key").orElse(UUID.randomUUID().toString());
    if (!COMMAND_ID.matcher(commandId).matches()) {
      throw ApiException.badRequest("commandId must match " + COMMAND_ID.pattern(), List.of());
    }
    CaseRecord current = find(caseId);
    if (current == null) {
      throw ApiException.notFound("Unknown case");
    }
    String actor = request.principal().name();
    CaseCommand command =
        new CaseCommand(
            commandId,
            current.accountId(),
            caseId,
            body.type(),
            actor,
            body.comment(),
            clock.millis());
    boolean replay = current.processedCommands().contains(commandId);
    if (!replay
        && CaseStateMachine.apply(current, command)
            instanceof CaseStateMachine.Outcome.Rejected rejected) {
      throw new ApiException(409, "illegal_transition", rejected.reason());
    }
    if (!replay) {
      publish(command);
    }
    return Response.json(
        202, Map.of("commandId", commandId, "caseId", caseId, "actor", actor, "replayed", replay));
  }

  private void publish(CaseCommand command) {
    try {
      producer
          .send(
              new ProducerRecord<>(topics.caseCommands(), command.accountId(), Json.write(command)))
          .get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw ApiException.unavailable("Interrupted while publishing");
    } catch (ExecutionException | TimeoutException e) {
      throw ApiException.unavailable("Could not publish to Kafka, retry later");
    }
  }
}
