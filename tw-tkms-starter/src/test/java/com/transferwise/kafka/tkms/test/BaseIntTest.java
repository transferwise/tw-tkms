package com.transferwise.kafka.tkms.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.kafka.tkms.TkmsClockHolder;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.awaitility.core.ConditionTimeoutException;
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

  @Autowired
  protected TestProperties testProperties;

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
      if (!(meter instanceof Gauge)) {
        meterRegistry.remove(meter);
      }
    }
    meterCache.clear();


    TkmsClockHolder.reset();
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

  protected void waitUntilTablesAreEmpty() {
    try {
      await().until(() -> getTablesRowsCount() == 0);
    } catch (ConditionTimeoutException ignored) {
      // To get a good cause message.
      assertThatTablesAreEmpty();
    }
  }

  protected void assertThatTablesAreEmpty() {
    for (int s = 0; s < tkmsProperties.getShardsCount(); s++) {
      for (int p = 0; p < tkmsProperties.getPartitionsCount(s); p++) {
        TkmsShardPartition sp = TkmsShardPartition.of(s, p);
        int rowsCount = tkmsTestDao.getMessagesCount(sp);

        assertThat(rowsCount).as("Row count for " + sp + " is zero.").isZero();
      }
    }
  }

}
