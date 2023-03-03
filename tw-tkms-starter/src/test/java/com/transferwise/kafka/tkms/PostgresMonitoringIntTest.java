package com.transferwise.kafka.tkms;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"test", "postgres"})
public class PostgresMonitoringIntTest extends MonitoringIntTest {

  @Test
  void testThatTableStatsMetricsArePresent() {
  }
}
