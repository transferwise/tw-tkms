package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.time.Duration;

public interface ITkmsPaceMaker {

  void doSmallPause(int shard);
  
  Duration getLongWaitTime(int shard);

  Duration getPollingPause(TkmsShardPartition tkmsShardPartition, int pollingBatchSize, int polledMessagesCount);

  Duration getPollingPauseOnError(TkmsShardPartition tkmsShardPartition);
}
