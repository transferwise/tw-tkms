package com.transferwise.kafka.tkms;

import java.time.Clock;

public class TkmsClockHolder {

  private static Clock clock = Clock.systemUTC();

  public static void setClock(Clock clock) {
    TkmsClockHolder.clock = clock;
  }

  public static void reset() {
    clock = Clock.systemUTC();
  }

  public static Clock getClock() {
    return clock;
  }
}
