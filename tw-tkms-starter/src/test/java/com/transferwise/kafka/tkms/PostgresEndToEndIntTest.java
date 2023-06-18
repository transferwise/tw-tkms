package com.transferwise.kafka.tkms;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"test", "postgres"})
public class PostgresEndToEndIntTest extends EndToEndIntTest {

  private static Stream<Arguments> compressionInput() {
    var deferUntilCommits = List.of(false, true);
    var arguments = new ArrayList<Arguments>();

    for (var deferUntilCommit : deferUntilCommits) {
      arguments.add(Arguments.of(CompressionAlgorithm.GZIP, 111, 110, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.NONE, 1171, 1171, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.LZ4, 134, 134, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.SNAPPY, 160, 160, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.SNAPPY_FRAMED, 158, 158, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.ZSTD, 100, 100, deferUntilCommit));
    }

    return arguments.stream();
  }

  @ParameterizedTest
  @MethodSource("compressionInput")
  @Override
  public void testMessageIsCompressed(CompressionAlgorithm algorithm, int expectedSerializedSize, int expectedSerializedSizeAlt,
      boolean deferUntilCommit) {
    super.testMessageIsCompressed(algorithm, expectedSerializedSize, expectedSerializedSizeAlt, deferUntilCommit);
  }
}
