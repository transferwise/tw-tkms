package com.transferwise.kafka.tkms;

import java.time.Duration;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class EarliestMessageSlidingWindow {

  private static final int BUCKETS_COUNT = 100;

  private long[] buckets;
  private long stepMs;
  private int idx;
  private long idxMs;
  private long initializationMs = -1;
  private long periodMs;

  public EarliestMessageSlidingWindow(Duration lookBackPeriod) {
    buckets = new long[BUCKETS_COUNT];
    periodMs = lookBackPeriod.toMillis();
    stepMs = periodMs / BUCKETS_COUNT;

    resetBuckets(TkmsClockHolder.getClock().millis());
  }

  public void register(long id) {
    scroll();

    if (initializationMs == -1) {
      initializationMs = TkmsClockHolder.getClock().millis();
    }

    if (id < buckets[idx]) {
      buckets[idx] = id;
    }
  }

  /*
   * Moving the bucket index forward and emptying slots which will be skipped.
   */
  private void scroll() {
    long timeMs = TkmsClockHolder.getClock().millis();

    // Too much time has passed, it is most optimal to reset everything.
    if (timeMs > idxMs + BUCKETS_COUNT * stepMs) {
      resetBuckets(timeMs);
      return;
    }

    while (timeMs > idxMs + stepMs) {
      idx += 1;
      idxMs += stepMs;

      if (idx >= BUCKETS_COUNT) {
        idx = 0;
      }

      buckets[idx] = Long.MAX_VALUE;
    }
  }

  private void resetBuckets(long timeMs) {
    idxMs = timeMs;
    idx = 0;
    for (int i = 0; i < BUCKETS_COUNT; i++) {
      buckets[i] = Long.MAX_VALUE;
    }
  }

  public long getEarliestMessageId() {
    if (TkmsClockHolder.getClock().millis() - initializationMs <= periodMs) {
      // We can't return an id, before first period finishes. We don't know what happened in the whole period yet,
      // maybe there was a smaller id present.
      return -1;
    }

    scroll();

    long earliestMessageId = Long.MAX_VALUE;
    for (int i = 0; i < BUCKETS_COUNT; i++) {
      if (buckets[i] < earliestMessageId) {
        earliestMessageId = buckets[i];
      }
    }
    return earliestMessageId == Long.MAX_VALUE ? -1 : earliestMessageId;
  }
}
