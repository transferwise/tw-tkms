package com.transferwise.kafka.tkms.test;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.kafka.tkms.TkmsClockHolder;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;

@BaseTestEnvironment
public class BaseIntTest {

  @Autowired
  protected ITkmsRegisteredMessagesCollector tkmsRegisteredMessagesCollector;

  @Autowired
  protected ITkmsSentMessagesCollector tkmsSentMessagesCollector;

  @Autowired
  protected MeterRegistry meterRegistry;

  @Autowired
  protected IMeterCache meterCache;

  @AfterEach
  public void cleanup() {
    tkmsRegisteredMessagesCollector.clear();
    tkmsSentMessagesCollector.clear();

    for (Meter meter : meterRegistry.getMeters()) {
      if (!(meter instanceof Gauge)) {
        meterRegistry.remove(meter);
      }
    }
    meterCache.clear();

    TkmsClockHolder.reset();
  }
}
