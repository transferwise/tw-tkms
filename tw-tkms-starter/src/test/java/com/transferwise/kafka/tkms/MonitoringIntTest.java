package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.test.BaseIntTest;
import io.micrometer.core.instrument.Gauge;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class MonitoringIntTest extends BaseIntTest {

  @Test
  void testThatMonitoringMetricsArePresent() {
    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find("tw.tkms.dao.approximate.messages.count").tags("shard", "0", "partition", "0").gauge();
      return gauge != null && gauge.value() >= 0;
    });

    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find("tw.tkms.dao.approximate.messages.count").tags("shard", "1", "partition", "0").gauge();
      return gauge != null && gauge.value() >= 0;
    });
  }

  @Test
  void testThatTableStatsMetricsArePresent() {
    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find("tw.tkms.dao.rows.in.table.stats").tags("shard", "1", "partition", "0").gauge();
      return gauge != null && gauge.value() == 1_000_000;
    });

    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find("tw.tkms.dao.rows.in.index.stats").tags("shard", "1", "partition", "0").gauge();
      return gauge != null && gauge.value() == 1_000_000;
    });
  }
}
