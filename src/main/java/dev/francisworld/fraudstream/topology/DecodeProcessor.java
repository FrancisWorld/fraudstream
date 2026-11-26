package dev.francisworld.fraudstream.topology;

import dev.francisworld.fraudstream.model.DeadLetter;
import dev.francisworld.fraudstream.schema.DecodeException;
import java.util.Base64;
import java.util.Optional;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes raw bytes at the edge of the topology. Failures never throw: they become a {@link
 * DeadLetter} so one poison record cannot stall a partition.
 */
final class DecodeProcessor<T> implements FixedKeyProcessor<String, byte[], Decoded<T>> {

  /** Turns bytes into a domain value or explains why it cannot. */
  @FunctionalInterface
  interface Decoder<T> {
    T decode(byte[] bytes) throws DecodeException;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DecodeProcessor.class);

  private final String source;
  private final Decoder<T> decoder;
  private final Runnable onDecoded;
  private final FraudMetrics metrics;
  private FixedKeyProcessorContext<String, Decoded<T>> context;

  DecodeProcessor(String source, Decoder<T> decoder, Runnable onDecoded, FraudMetrics metrics) {
    this.source = source;
    this.decoder = decoder;
    this.onDecoded = onDecoded;
    this.metrics = metrics;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, Decoded<T>> context) {
    this.context = context;
  }

  @Override
  public void process(FixedKeyRecord<String, byte[]> record) {
    Decoded<T> result;
    try {
      result = Decoded.ok(decoder.decode(record.value()));
      onDecoded.run();
    } catch (DecodeException e) {
      Optional<RecordMetadata> meta = context.recordMetadata();
      byte[] payload = record.value() == null ? new byte[0] : record.value();
      result =
          Decoded.failed(
              new DeadLetter(
                  e.reason(),
                  e.getMessage(),
                  meta.map(RecordMetadata::topic).orElse(source),
                  meta.map(RecordMetadata::partition).orElse(-1),
                  meta.map(RecordMetadata::offset).orElse(-1L),
                  record.timestamp(),
                  Base64.getEncoder().encodeToString(payload)));
      metrics.deadLetter(source, e.reason());
      LOG.warn("Dead-lettering {} record: {} ({})", source, e.getMessage(), e.reason());
    }
    context.forward(record.withValue(result));
  }
}
