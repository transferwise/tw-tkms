package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.time.Duration;

public interface ITkmsPaceMaker {

  void doSmallPause(int shard);

  Duration getProxyStopTimeout(TkmsShardPartition tkmsShardPartition);

  Duration getPollingPause(TkmsShardPartition tkmsShardPartition, int pollingBatchSize, int polledMessagesCount);

  Duration getPollingPauseOnError(TkmsShardPartition tkmsShardPartition);
}
