package com.transferwise.kafka.tkms.test;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.kafka.tkms.TkmsClockHolder;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

  @Autowired
  protected TkmsProperties tkmsProperties;
  
  @Autowired
  protected TkmsTestDao tkmsTestDao;

  @AfterEach
  public void cleanup() {
    tkmsRegisteredMessagesCollector.clear();
    tkmsSentMessagesCollector.clear();

    TkmsClockHolder.reset();
    TestLogAppender.reset();
  }

  @BeforeEach
  public void setup() {
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

  protected int getTablesRowsCount() {
    int count = 0;
    for (int s = 0; s < tkmsProperties.getShardsCount(); s++) {
      for (int p = 0; p < tkmsProperties.getPartitionsCount(s); p++) {
        TkmsShardPartition sp = TkmsShardPartition.of(s, p);
        count += tkmsTestDao.getMessagesCount(sp);
      }
    }
    return count;
  }
}
