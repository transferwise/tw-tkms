package com.transferwise.kafka.tkms.test;

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

  @AfterEach
  public void cleanup() {
    tkmsRegisteredMessagesCollector.clear();
    tkmsSentMessagesCollector.clear();
    meterRegistry.clear();
  }
}
