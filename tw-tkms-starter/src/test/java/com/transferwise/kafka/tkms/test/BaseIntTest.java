package com.transferwise.kafka.tkms.test;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.common.baseutils.meters.cache.MeterCache;
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
  
  @Autowired
  protected IMeterCache meterCache;

  @AfterEach
  public void cleanup() {
    tkmsRegisteredMessagesCollector.clear();
    tkmsSentMessagesCollector.clear();
    meterRegistry.clear();
    meterCache.clear();
  }
}
