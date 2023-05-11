package com.transferwise.kafka.tkms;

import java.time.Clock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TkmsClockHolder {

  private static Clock clock = Clock.systemUTC();

  public static void setClock(Clock clock) {
    if (Debug.isEarliestMessagesTrackerDebugEnabled()) {
      log.info("Setting clock to '{}' with millis {}.", clock, clock.millis());
    }
    TkmsClockHolder.clock = clock;
  }

  public static void reset() {
    clock = Clock.systemUTC();
  }

  public static Clock getClock() {
    return clock;
  }

  private TkmsClockHolder() {

  }
}
