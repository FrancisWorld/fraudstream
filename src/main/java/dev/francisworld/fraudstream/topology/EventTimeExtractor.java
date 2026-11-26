package dev.francisworld.fraudstream.topology;

import dev.francisworld.fraudstream.schema.DecodeException;
import dev.francisworld.fraudstream.schema.TransactionCodec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Uses the transaction's own event time instead of the broker append time, so windows and the grace
 * period are measured in the time the payment happened. Anything that is not a decodable
 * transaction falls back to the record timestamp.
 */
public final class EventTimeExtractor implements TimestampExtractor {

  private final TransactionCodec codec;

  public EventTimeExtractor(TransactionCodec codec) {
    this.codec = codec;
  }

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    if (record.value() instanceof byte[] bytes) {
      try {
        return codec.decode(bytes).eventTimeMs();
      } catch (DecodeException e) {
        // Poison records are dead-lettered downstream; here they just need a sane timestamp.
      }
    }
    return record.timestamp() >= 0 ? record.timestamp() : Math.max(partitionTime, 0);
  }
}
