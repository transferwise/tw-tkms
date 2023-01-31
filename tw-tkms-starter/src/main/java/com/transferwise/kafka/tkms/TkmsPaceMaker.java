package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
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
  public Duration getLongWaitTime(int shard) {
    return Duration.ofSeconds(15);
  }

  @Override
  public Duration getPollingPause(TkmsShardPartition shardPartition, int pollingBatchSize, int polledMessagesCount) {
    long maxPollIntervalMs = properties.getPollingInterval(shardPartition.getShard()).toMillis();

    return Duration.ofMillis(maxPollIntervalMs * (pollingBatchSize - polledMessagesCount) / pollingBatchSize);
  }

  @Override
  public Duration getPollingPauseOnError(TkmsShardPartition shardPartition) {
    return properties.getPauseTimeOnErrors(shardPartition.getShard());
  }
}
