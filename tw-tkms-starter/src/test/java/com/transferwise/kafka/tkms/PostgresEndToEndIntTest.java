package com.transferwise.kafka.tkms;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"test", "postgres"})
public class PostgresEndToEndIntTest extends EndToEndIntTest {

  private static Stream<Arguments> compressionInput() {
    return Stream.of(
        Arguments.of(CompressionAlgorithm.GZIP, 111, 110),
        Arguments.of(CompressionAlgorithm.NONE, 1171, 1171),
        Arguments.of(CompressionAlgorithm.LZ4, 134, 134),
        Arguments.of(CompressionAlgorithm.SNAPPY, 160, 160),
        Arguments.of(CompressionAlgorithm.SNAPPY_FRAMED, 158, 158),
        Arguments.of(CompressionAlgorithm.ZSTD, 100, 100)
    );
  }

  @ParameterizedTest
  @MethodSource("compressionInput")
  @Override
  public void testMessageIsCompressed(CompressionAlgorithm algorithm, int expectedSerializedSize, int expectedSerializedSizeAlt) {
    super.testMessageIsCompressed(algorithm, expectedSerializedSize, expectedSerializedSizeAlt);
  }
}
