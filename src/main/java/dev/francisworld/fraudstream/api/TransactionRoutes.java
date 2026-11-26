package dev.francisworld.fraudstream.api;

import dev.francisworld.fraudstream.schema.TransactionCodec;
import dev.francisworld.fraudstream.schema.TransactionRequestValidator;
import dev.francisworld.fraudstream.topology.Topics;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * {@code POST /api/v1/transactions}: validate, encode in the caller's schema generation, publish.
 */
public final class TransactionRoutes {

  private static final long SEND_TIMEOUT_SECONDS = 5;

  private final Producer<String, byte[]> producer;
  private final Topics topics;
  private final TransactionRequestValidator validator;
  private final TransactionCodec codec;

  public TransactionRoutes(
      Producer<String, byte[]> producer,
      Topics topics,
      TransactionRequestValidator validator,
      TransactionCodec codec) {
    this.producer = producer;
    this.topics = topics;
    this.validator = validator;
    this.codec = codec;
  }

  public void register(Router router) {
    router.route("POST", "/api/v1/transactions", Permission.INGEST_TRANSACTIONS, this::ingest);
  }

  private Response ingest(Request request) {
    TransactionRequestValidator.Result result = validator.validate(request.bodyText());
    if (!result.errors().isEmpty()) {
      throw new ApiException(
          422, "validation_failed", "Transaction does not match the schema", result.errors());
    }
    var tx = result.transaction();
    byte[] payload = codec.encode(tx, result.schemaVersion());
    try {
      // Keyed by account so one account's events stay ordered within a partition.
      RecordMetadata meta =
          producer
              .send(new ProducerRecord<>(topics.transactions(), tx.accountId(), payload))
              .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      return Response.json(
          202,
          Map.of(
              "transactionId", tx.transactionId(),
              "schemaVersion", result.schemaVersion(),
              "partition", meta.partition(),
              "offset", meta.offset()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw ApiException.unavailable("Interrupted while publishing");
    } catch (ExecutionException | TimeoutException e) {
      throw ApiException.unavailable("Could not publish to Kafka, retry later");
    }
  }
}
