package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;

public class TkmsPaceMaker implements ITkmsPaceMaker {

  @Autowired
  private TkmsProperties properties;

  @Override
  public void doSmallPause(int shard) {
    ExceptionUtils.doUnchecked(() -> {
      Duration minPollingInterval = properties.getMinPollingInterval();
      Duration shardPollingInterval = properties.getPollingInterval(shard);

      if (minPollingInterval != null && minPollingInterval.compareTo(shardPollingInterval) > 0) {
        Thread.sleep(minPollingInterval.toMillis());
      } else {
        Thread.sleep(shardPollingInterval.toMillis());
      }
    });
  }

  @Override
  public void pauseOnError(int shard) {
    ExceptionUtils.doUnchecked(() -> Thread.sleep(properties.getPauseTimeOnErrors(shard).toMillis()));
  }

  @Override
  public Duration getLongWaitTime(int shard) {
    return Duration.ofSeconds(15);
  }
}
