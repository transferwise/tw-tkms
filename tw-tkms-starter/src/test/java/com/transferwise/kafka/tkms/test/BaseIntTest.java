package com.transferwise.kafka.tkms.test;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.kafka.tkms.TkmsClockHolder;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

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
  
  @Autowired
  protected JdbcTemplate jdbcTemplate;

  @AfterEach
  public void cleanup() {
    tkmsRegisteredMessagesCollector.clear();
    tkmsSentMessagesCollector.clear();

    for (Meter meter : meterRegistry.getMeters()) {
      if (!(meter instanceof Gauge) && !(isKafkaProducerMeter(meter))) {
        meterRegistry.remove(meter);
      }
    }
    meterCache.clear();

    TkmsClockHolder.reset();
  }

  private boolean isKafkaProducerMeter(Meter meter) {
    return meter.getId().getName().startsWith("kafka.producer.");
  }
}
