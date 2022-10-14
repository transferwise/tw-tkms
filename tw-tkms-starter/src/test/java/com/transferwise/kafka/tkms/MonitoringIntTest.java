package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.kafka.tkms.test.BaseIntTest;
import io.micrometer.core.instrument.Gauge;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

@Slf4j
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

  @Test
  void earliestMessageIdIsRegistered() {
    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find("tw.tkms.dao.earliest.message.id").tags("shard", "1", "partition", "0").gauge();
      return gauge != null;
    });

    assertThat(meterRegistry.find("tw.tkms.dao.earliest.message.id").tags("shard", "0", "partition", "0").gauge())
        .as("Earliest message id tracking is not enabled for shard 0.").isNull();
  }
}
