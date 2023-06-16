package com.transferwise.kafka.tkms.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import lombok.Getter;
import lombok.Setter;

public class TestLogAppender extends AppenderBase<ILoggingEvent> {

  @Getter
  private static AtomicLong errorsCount = new AtomicLong();

  @Setter
  private static volatile Predicate<ILoggingEvent> eventFilter;

  @Override
  protected void append(ILoggingEvent event) {
    if (eventFilter != null && !eventFilter.test(event)) {
      return;
    }

    if (event.getLevel() == Level.ERROR) {
      errorsCount.incrementAndGet();
    }
  }

  public static void reset() {
    eventFilter = null;
    errorsCount.set(0);
  }
}
