package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.kafka.tkms.config.ITkmsDaoProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import org.springframework.beans.factory.annotation.Autowired;

public class TkmsMessagePollerFactory implements ITkmsMessagePollerFactory {

  @Autowired
  private ITkmsDaoProvider tkmsDaoProvider;
  @Autowired
  private IExecutorServicesProvider executorServicesProvider;
  @Autowired
  private TkmsProperties properties;

  @Override
  public TkmsMessagePoller createPoller(int shard) {
    int pollerParallelism = properties.getPollerParallelism(shard);
    return new TkmsMessagePoller(tkmsDaoProvider, executorServicesProvider, pollerParallelism);
  }
}
