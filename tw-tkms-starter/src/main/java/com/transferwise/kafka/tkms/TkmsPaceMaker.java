package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;

public class TkmsPaceMaker implements ITkmsPaceMaker {

  @Autowired
  protected TkmsProperties properties;

  @Override
  public void doSmallPause(int shard) {
    ExceptionUtils.doUnchecked(() -> {
      var minPollingInterval = properties.getMinPollingInterval();
      var shardPollingInterval = properties.getPollingInterval(shard);

      if (minPollingInterval != null && minPollingInterval.compareTo(shardPollingInterval) > 0) {
        Thread.sleep(minPollingInterval.toMillis());
      } else {
        Thread.sleep(shardPollingInterval.toMillis());
      }
    });
  }
  
  @Override
  public Duration getProxyStopTimeout(TkmsShardPartition shardPartition) {
    return properties.getProxyStopTimeout(shardPartition.getShard());
  }

  @Override
  public Duration getPollingPause(TkmsShardPartition shardPartition, int pollingBatchSize, int polledMessagesCount) {
    var maxPollInterval = properties.getPollingInterval(shardPartition.getShard());

    var minPollingInterval = properties.getMinPollingInterval();
    if (minPollingInterval != null && minPollingInterval.compareTo(maxPollInterval) > 0) {
      maxPollInterval = minPollingInterval;
    }

    return Duration.ofMillis(maxPollInterval.toMillis() * (pollingBatchSize - polledMessagesCount) / pollingBatchSize);
  }

  @Override
  public Duration getPollingPauseOnError(TkmsShardPartition shardPartition) {
    return properties.getPauseTimeOnErrors(shardPartition.getShard());
  }
}
