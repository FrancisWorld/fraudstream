package dev.francisworld.fraudstream.schema;

import static dev.francisworld.fraudstream.support.Txs.tx;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.francisworld.fraudstream.model.Transaction;
import java.nio.ByteBuffer;
import java.util.Arrays;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.junit.jupiter.api.Test;

class TransactionCodecTest {

  private final SchemaRegistry registry = SchemaRegistry.load();
  private final TransactionCodec codec = new TransactionCodec(registry);

  @Test
  void latestGenerationRoundTripsEveryField() throws DecodeException {
    Transaction original = tx("t-1").account("a-1").at(-23.5, -46.6).device("dev-1").build();
    byte[] bytes = codec.encode(original, SchemaRegistry.V2);
    assertThat(bytes[0]).isZero();
    assertThat(ByteBuffer.wrap(bytes, 1, 4).getInt()).isEqualTo(2);
    assertThat(codec.decode(bytes)).isEqualTo(original);
  }

  @Test
  void generationOneDataIsReadableByTheLatestReaderWithDefaults() throws DecodeException {
    Transaction withDevice = tx("t-1").device("dev-1").build();
    Transaction decoded = codec.decode(codec.encode(withDevice, SchemaRegistry.V1));
    assertThat(decoded.deviceId()).isNull();
    assertThat(decoded.ipAddress()).isNull();
    assertThat(decoded.channel()).isEqualTo("unknown");
    assertThat(decoded.transactionId()).isEqualTo("t-1");
    assertThat(decoded.amountMinor()).isEqualTo(withDevice.amountMinor());
  }

  @Test
  void schemaGenerationsAreMutuallyCompatible() {
    Schema v1 = registry.find(SchemaRegistry.V1).orElseThrow();
    Schema v2 = registry.find(SchemaRegistry.V2).orElseThrow();
    // backward: the new reader understands old data
    assertThat(SchemaCompatibility.checkReaderWriterCompatibility(v2, v1).getType())
        .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
    // forward: a not-yet-upgraded reader understands new data
    assertThat(SchemaCompatibility.checkReaderWriterCompatibility(v1, v2).getType())
        .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
    assertThat(registry.latestId()).isEqualTo(2);
    assertThat(registry.all()).containsOnlyKeys(1, 2);
  }

  @Test
  void rejectsMalformedEnvelopes() {
    assertThatThrownBy(() -> codec.decode(null))
        .isInstanceOfSatisfying(
            DecodeException.class, e -> assertThat(e.reason()).isEqualTo("bad-envelope"));
    assertThatThrownBy(() -> codec.decode(new byte[] {0, 0})).isInstanceOf(DecodeException.class);
    assertThatThrownBy(() -> codec.decode(new byte[] {7, 0, 0, 0, 1, 1}))
        .isInstanceOfSatisfying(
            DecodeException.class, e -> assertThat(e.reason()).isEqualTo("bad-envelope"));
    assertThatThrownBy(() -> codec.decode(new byte[] {0, 0, 0, 0, 42, 1}))
        .isInstanceOfSatisfying(
            DecodeException.class, e -> assertThat(e.reason()).isEqualTo("unknown-schema"));
  }

  @Test
  void truncatedPayloadsAreCorruptNotCrashes() {
    byte[] good = codec.encode(tx("t").build(), SchemaRegistry.V2);
    byte[] truncated = Arrays.copyOf(good, good.length - 6);
    assertThatThrownBy(() -> codec.decode(truncated))
        .isInstanceOfSatisfying(
            DecodeException.class, e -> assertThat(e.reason()).isEqualTo("corrupt-payload"));
  }

  @Test
  void encodingWithAnUnknownSchemaIdIsAProgrammingError() {
    assertThatThrownBy(() -> codec.encode(tx("t").build(), 9))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Provide
  Arbitrary<Transaction> transactions() {
    Arbitrary<String> ids =
        Arbitraries.strings().withChars("abcxyz019-_.:").ofMinLength(1).ofMaxLength(20);
    return Combinators.combine(
            ids,
            ids,
            Arbitraries.longs().between(1, 1_000_000_000L),
            Arbitraries.doubles().between(-90, 90),
            Arbitraries.doubles().between(-180, 180),
            Arbitraries.longs().between(0, 4_000_000_000_000L),
            Arbitraries.of(true, false))
        .as((id, account, amount, lat, lon, ts, located) ->
            new Transaction(
                id,
                account,
                amount,
                "USD",
                "m-" + id,
                "5999",
                "US",
                located ? lat : null,
                located ? lon : null,
                ts,
                located ? "dev-" + id : null,
                null,
                located ? "web" : null));
  }

  @Property(tries = 300)
  void anyValidTransactionSurvivesTheLatestWireFormat(@ForAll("transactions") Transaction tx)
      throws DecodeException {
    Transaction decoded = codec.decode(codec.encode(tx, SchemaRegistry.V2));
    assertThat(decoded.transactionId()).isEqualTo(tx.transactionId());
    assertThat(decoded.amountMinor()).isEqualTo(tx.amountMinor());
    assertThat(decoded.latitude()).isEqualTo(tx.latitude());
    assertThat(decoded.deviceId()).isEqualTo(tx.deviceId());
    assertThat(decoded.eventTimeMs()).isEqualTo(tx.eventTimeMs());
  }
}
