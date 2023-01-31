package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.kafka.tkms.metrics.TkmsMetricsTemplate;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import io.micrometer.core.instrument.Gauge;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class MonitoringIntTest extends BaseIntTest {

  @Test
  void testThatMonitoringMetricsArePresent() {
    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find(TkmsMetricsTemplate.DAO_APPROXIMATE_MESSAGES_COUNT).tags("shard", "0", "partition", "0").gauge();
      return gauge != null && gauge.value() >= 0;
    });

    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find(TkmsMetricsTemplate.DAO_APPROXIMATE_MESSAGES_COUNT).tags("shard", "1", "partition", "0").gauge();
      return gauge != null && gauge.value() >= 0;
    });
  }

  @Test
  void testThatTableStatsMetricsArePresent() {
    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find(TkmsMetricsTemplate.DAO_ROWS_IN_TABLE_STATS).tags("shard", "1", "partition", "0").gauge();
      return gauge != null && gauge.value() == 1_000_000;
    });

    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find(TkmsMetricsTemplate.DAO_ROWS_IN_INDEX_STATS).tags("shard", "1", "partition", "0").gauge();
      return gauge != null && gauge.value() == 1_000_000;
    });
  }

  @Test
  void earliestMessageIdIsRegistered() {
    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find(TkmsMetricsTemplate.DAO_EARLIEST_MESSAGE_ID).tags("shard", "1", "partition", "0").gauge();
      return gauge != null;
    });

    assertThat(meterRegistry.find(TkmsMetricsTemplate.DAO_EARLIEST_MESSAGE_ID).tags("shard", "0", "partition", "0").gauge())
        .as("Earliest message id tracking is not enabled for shard 0.").isNull();
  }
}
