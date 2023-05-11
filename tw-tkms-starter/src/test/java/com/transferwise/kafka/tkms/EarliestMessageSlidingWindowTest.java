package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class EarliestMessageSlidingWindowTest extends BaseIntTest {

  @Test
  void testSlidingWindow() {
    TestClock clock = new TestClock(Instant.now());
    TkmsClockHolder.setClock(clock);

    EarliestMessageSlidingWindow slidingWindow = new EarliestMessageSlidingWindow(Duration.ofSeconds(10));

    // tick, current message id, expected earliest message id.
    long[][] values = new long[][]{
        {0, 9, -1},
        {1, 12, -1},
        {1000, 3, -1},
        {5000, 88, -1},
        {2000, 5, -1},
        {1999, 7, -1},
        {1, 7, 3}, // First period finishes
        {1000, 8, 5},
        {6999, 9, 5},
        {1, 9, 7}};

    assertThat(slidingWindow.getEarliestMessageId()).isEqualTo(-1);

    for (long[] value : values) {
      clock.tick(Duration.ofMillis(value[0]));
      slidingWindow.register(value[1]);
      assertThat(slidingWindow.getEarliestMessageId()).isEqualTo(value[2]);
    }

    clock.tick(Duration.ofMillis(9999));
    assertThat(slidingWindow.getEarliestMessageId()).isEqualTo(9);

    clock.tick(Duration.ofMillis(1));
    assertThat(slidingWindow.getEarliestMessageId()).isEqualTo(-1);

    clock.tick(Duration.ofSeconds(10));
    assertThat(slidingWindow.getEarliestMessageId()).isEqualTo(-1);

    slidingWindow.register(13);
    assertThat(slidingWindow.getEarliestMessageId()).isEqualTo(13);
  }
}
