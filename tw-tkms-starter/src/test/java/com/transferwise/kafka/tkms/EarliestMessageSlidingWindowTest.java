package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public class EarliestMessageSlidingWindowTest extends BaseIntTest {

  @Test
  void testSlidingWindow() {
    TestClock clock = new TestClock();
    TkmsClockHolder.setClock(clock);

    EarliestMessageSlidingWindow slidingWindow = new EarliestMessageSlidingWindow(Duration.ofSeconds(10));

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

    for (int i = 0; i < values.length; i++) {
      clock.tick(Duration.ofMillis(values[i][0]));
      slidingWindow.register(values[i][1]);
      assertThat(slidingWindow.getEarliestMessageId()).isEqualTo(values[i][2]);
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
