package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class MetricsTemplateIntTest extends BaseIntTest {

  @Autowired
  private ITkmsMetricsTemplate metricsTemplate;

  // Production bug coverage
  @Test
  void proxyMessageSendFailureAfterSuccessCanBeRegistered() {
    metricsTemplate.recordProxyMessageSendSuccess(TkmsShardPartition.of(0, 0), "Topic", Instant.now());
    metricsTemplate.recordProxyMessageSendFailure(TkmsShardPartition.of(0, 0), "Topic");
  }
}
