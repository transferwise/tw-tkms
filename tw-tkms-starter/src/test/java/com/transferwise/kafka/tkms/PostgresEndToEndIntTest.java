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
        Arguments.of(CompressionAlgorithm.GZIP, 112),
        Arguments.of(CompressionAlgorithm.NONE, 1271),
        Arguments.of(CompressionAlgorithm.LZ4, 135),
        Arguments.of(CompressionAlgorithm.SNAPPY, 167),
        Arguments.of(CompressionAlgorithm.SNAPPY_FRAMED, 165),
        Arguments.of(CompressionAlgorithm.ZSTD, 101)
    );
  }

  @ParameterizedTest
  @MethodSource("compressionInput")
  public void testMessageIsCompressed(CompressionAlgorithm algorithm, int expectedSerializedSize) throws Exception {
    super.testMessageIsCompressed(algorithm, expectedSerializedSize);
  }
}
