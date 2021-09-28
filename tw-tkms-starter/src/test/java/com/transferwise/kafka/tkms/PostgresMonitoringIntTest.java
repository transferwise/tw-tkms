package com.transferwise.kafka.tkms;

import io.micrometer.core.instrument.Gauge;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"test", "postgres"})
public class PostgresMonitoringIntTest extends MonitoringIntTest {

  @Test
  void testThatTableStatsMetricsArePresent() {
    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find("tw.tkms.dao.rows.in.table.stats").tags("shard", "1", "partition", "0").gauge();
      return gauge != null && gauge.value() == 1_000_000;
    });
  }
}
