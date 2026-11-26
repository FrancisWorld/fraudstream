package dev.francisworld.fraudstream.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.Error;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import dev.francisworld.fraudstream.model.Transaction;
import dev.francisworld.fraudstream.serde.Json;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

/**
 * Validates ingestion requests against the versioned JSON Schemas and maps them to the domain
 * model. A request without {@code schemaVersion} is treated as generation 1 so older producers keep
 * working while new ones opt into generation 2.
 */
public final class TransactionRequestValidator {

  /** Outcome of validation: either a transaction or a non-empty list of errors. */
  public record Result(int schemaVersion, Transaction transaction, List<String> errors) {
    static Result failed(String... errors) {
      return new Result(0, null, List.of(errors));
    }
  }

  private final Map<Integer, Schema> schemas;

  public TransactionRequestValidator() {
    var registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
    this.schemas =
        Map.of(
            1, load(registry, "/schemas/transaction-request-v1.json"),
            2, load(registry, "/schemas/transaction-request-v2.json"));
  }

  private static Schema load(SchemaRegistry registry, String resource) {
    try (InputStream in = TransactionRequestValidator.class.getResourceAsStream(resource)) {
      if (in == null) {
        throw new IllegalStateException("Missing schema " + resource);
      }
      return registry.getSchema(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Result validate(String body) {
    JsonNode root;
    try {
      root = Json.MAPPER.readTree(body);
    } catch (IOException e) {
      return Result.failed("Body is not valid JSON");
    }
    if (root == null || !root.isObject()) {
      return Result.failed("Body must be a JSON object");
    }
    int version = root.path("schemaVersion").asInt(1);
    Schema schema = schemas.get(version);
    if (schema == null) {
      return Result.failed("Unsupported schemaVersion " + root.path("schemaVersion").asText());
    }
    List<Error> violations = schema.validate(body, InputFormat.JSON);
    if (!violations.isEmpty()) {
      return new Result(
          version, null, violations.stream().map(Error::getMessage).sorted().toList());
    }
    try {
      return new Result(version, toTransaction(root), List.of());
    } catch (DateTimeParseException | IllegalArgumentException e) {
      return Result.failed(e.getMessage());
    }
  }

  private static Transaction toTransaction(JsonNode n) {
    long eventTimeMs = OffsetDateTime.parse(n.get("eventTime").asText()).toInstant().toEpochMilli();
    return new Transaction(
        n.get("transactionId").asText(),
        n.get("accountId").asText(),
        n.get("amountMinor").asLong(),
        n.get("currency").asText(),
        n.get("merchantId").asText(),
        n.get("merchantCategory").asText(),
        text(n, "country"),
        n.has("latitude") ? n.get("latitude").asDouble() : null,
        n.has("longitude") ? n.get("longitude").asDouble() : null,
        eventTimeMs,
        text(n, "deviceId"),
        text(n, "ipAddress"),
        text(n, "channel"));
  }

  private static String text(JsonNode n, String field) {
    return n.has(field) ? n.get(field).asText() : null;
  }
}
