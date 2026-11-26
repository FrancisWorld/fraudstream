package dev.francisworld.fraudstream.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class TransactionRequestValidatorTest {

  private final TransactionRequestValidator validator = new TransactionRequestValidator();

  private static final String V1 =
      """
      {"transactionId":"t-1","accountId":"a-1","amountMinor":1250,"currency":"BRL",
       "merchantId":"m-1","merchantCategory":"5411","eventTime":"2026-03-02T12:00:00Z"}\
      """;

  @Test
  void acceptsAGenerationOneRequestWithoutSchemaVersion() {
    var result = validator.validate(V1);
    assertThat(result.errors()).isEmpty();
    assertThat(result.schemaVersion()).isEqualTo(1);
    assertThat(result.transaction().eventTimeMs()).isEqualTo(1_772_452_800_000L);
  }

  @Test
  void acceptsAGenerationTwoRequestWithDeviceAndLocation() {
    var result =
        validator.validate(
            """
            {"schemaVersion":2,"transactionId":"t-1","accountId":"a-1","amountMinor":1250,
             "currency":"BRL","merchantId":"m-1","merchantCategory":"5411","country":"BR",
             "latitude":-23.5,"longitude":-46.6,"eventTime":"2026-03-02T12:00:00-03:00",
             "deviceId":"dev-1","ipAddress":"203.0.113.9","channel":"mobile"}\
            """);
    assertThat(result.errors()).isEmpty();
    assertThat(result.schemaVersion()).isEqualTo(2);
    assertThat(result.transaction().deviceId()).isEqualTo("dev-1");
    assertThat(result.transaction().eventTimeMs()).isEqualTo(1_772_463_600_000L);
  }

  @Test
  void generationOneRejectsGenerationTwoFields() {
    var result =
        validator.validate(V1.replace("\"eventTime\"", "\"deviceId\":\"d\",\"eventTime\""));
    assertThat(result.errors()).isNotEmpty();
    assertThat(result.transaction()).isNull();
  }

  @Test
  void reportsAllViolations() {
    List<String> errors =
        validator
            .validate("{\"transactionId\":\"bad id!\",\"amountMinor\":-5,\"currency\":\"brl\"}")
            .errors();
    assertThat(errors.size()).isGreaterThanOrEqualTo(4);
  }

  @Test
  void latitudeRequiresLongitude() {
    var result = validator.validate(V1.replace("\"eventTime\"", "\"latitude\":10,\"eventTime\""));
    assertThat(result.errors()).isNotEmpty();
  }

  @Test
  void rejectsUnsupportedSchemaVersionsAndNonObjects() {
    assertThat(validator.validate(V1.replace("{", "{\"schemaVersion\":3,")).errors()).isNotEmpty();
    assertThat(validator.validate("[1,2]").errors()).isNotEmpty();
    assertThat(validator.validate("not json").errors()).singleElement().asString().contains("JSON");
    assertThat(validator.validate("").errors()).isNotEmpty();
  }

  @Test
  void rejectsUnparseableTimestampsThatSlippedPastTheFormatCheck() {
    var result = validator.validate(V1.replace("2026-03-02T12:00:00Z", "2026-13-45T99:00:00Z"));
    assertThat(result.errors()).isNotEmpty();
  }

  @Test
  void rejectsTimestampsBeforeTheEpoch() {
    var result = validator.validate(V1.replace("2026-03-02T12:00:00Z", "1960-01-01T00:00:00Z"));
    assertThat(result.errors()).isNotEmpty();
  }
}
