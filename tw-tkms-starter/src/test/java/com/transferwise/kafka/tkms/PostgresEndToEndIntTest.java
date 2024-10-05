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
      arguments.add(Arguments.of(CompressionAlgorithm.GZIP, 164, 165, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.NONE, 1226, 1226, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.LZ4, 190, 190, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.SNAPPY, 216, 216, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.SNAPPY_FRAMED, 214, 214, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.ZSTD, 155, 155, deferUntilCommit));
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
