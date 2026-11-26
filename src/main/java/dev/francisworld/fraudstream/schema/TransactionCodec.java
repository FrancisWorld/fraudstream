package dev.francisworld.fraudstream.schema;

import dev.francisworld.fraudstream.model.Transaction;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/**
 * Encodes transactions as {@code [0x00][schema id: int32][avro binary]} and decodes any known
 * generation into the latest in-memory model using Avro schema resolution.
 */
public final class TransactionCodec {

  private static final byte MAGIC = 0;
  private static final int HEADER = 5;

  private final SchemaRegistry registry;

  public TransactionCodec(SchemaRegistry registry) {
    this.registry = registry;
  }

  public byte[] encode(Transaction tx, int schemaId) {
    Schema schema =
        registry
            .find(schemaId)
            .orElseThrow(() -> new IllegalArgumentException("Unknown schema id " + schemaId));
    GenericRecord record = new GenericData.Record(schema);
    record.put("transactionId", tx.transactionId());
    record.put("accountId", tx.accountId());
    record.put("amountMinor", tx.amountMinor());
    record.put("currency", tx.currency());
    record.put("merchantId", tx.merchantId());
    record.put("merchantCategory", tx.merchantCategory());
    record.put("country", tx.country());
    record.put("latitude", tx.latitude());
    record.put("longitude", tx.longitude());
    record.put("eventTimeMs", tx.eventTimeMs());
    if (schema.getField("deviceId") != null) {
      record.put("deviceId", tx.deviceId());
      record.put("ipAddress", tx.ipAddress());
      record.put("channel", tx.channel() == null ? "unknown" : tx.channel());
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(MAGIC);
    out.writeBytes(ByteBuffer.allocate(4).putInt(schemaId).array());
    try {
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new IllegalStateException("Avro encoding failed", e);
    }
    return out.toByteArray();
  }

  public Transaction decode(byte[] bytes) throws DecodeException {
    if (bytes == null || bytes.length < HEADER || bytes[0] != MAGIC) {
      throw new DecodeException("bad-envelope", "Missing magic byte or schema id", null);
    }
    int schemaId = ByteBuffer.wrap(bytes, 1, 4).getInt();
    Schema writer =
        registry
            .find(schemaId)
            .orElseThrow(
                () -> new DecodeException("unknown-schema", "Unknown schema id " + schemaId, null));
    try {
      GenericDatumReader<GenericRecord> reader =
          new GenericDatumReader<>(writer, registry.latest());
      GenericRecord record =
          reader.read(
              null, DecoderFactory.get().binaryDecoder(bytes, HEADER, bytes.length - HEADER, null));
      return toTransaction(record);
    } catch (IOException | RuntimeException e) {
      throw new DecodeException("corrupt-payload", "Cannot decode payload: " + e.getMessage(), e);
    }
  }

  private static Transaction toTransaction(GenericRecord r) {
    return new Transaction(
        str(r.get("transactionId")),
        str(r.get("accountId")),
        (Long) r.get("amountMinor"),
        str(r.get("currency")),
        str(r.get("merchantId")),
        str(r.get("merchantCategory")),
        str(r.get("country")),
        (Double) r.get("latitude"),
        (Double) r.get("longitude"),
        (Long) r.get("eventTimeMs"),
        str(r.get("deviceId")),
        str(r.get("ipAddress")),
        str(r.get("channel")));
  }

  private static String str(Object value) {
    return value == null ? null : value.toString();
  }
}
