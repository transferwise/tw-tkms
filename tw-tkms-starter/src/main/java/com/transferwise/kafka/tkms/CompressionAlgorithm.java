package com.transferwise.kafka.tkms;

import io.micrometer.core.instrument.Tag;
import java.util.concurrent.ThreadLocalRandom;

public enum CompressionAlgorithm {
  NONE,
  /**
   * Recommended default.
   *
   * <p>Almost no memory allocations on output. Memory allocations for input correlate well with message sizes.
   */
  SNAPPY,
  /**
   * Deprecated, will be soon removed.
   */
  SNAPPY_FRAMED,
  /**
   * Extremely memory hungry, allocates 128kb buffers for both output and input.
   *
   * <p>Makes only sense with very large messages, unless we find a better implementation.
   */
  ZSTD,
  /**
   * Considerably faster than Snappy with similar compression rate.
   */
  LZ4,
  /**
   * Best compression rate with reasonable resource usage.
   */
  GZIP,
  // For complex tests
  RANDOM;

  private Tag micrometerTag = Tag.of("algorithm", name().toLowerCase());

  public Tag getMicrometerTag() {
    return micrometerTag;
  }

  public static CompressionAlgorithm getRandom() {
    switch (ThreadLocalRandom.current().nextInt(4)) {
      case 0:
        return NONE;
      case 1:
        return SNAPPY;
      case 2:
        return LZ4;
      case 3:
        return GZIP;
      default:
        return ZSTD;
    }
  }
}
