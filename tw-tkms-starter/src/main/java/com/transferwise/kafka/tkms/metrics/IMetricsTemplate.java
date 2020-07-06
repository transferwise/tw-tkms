package com.transferwise.kafka.tkms.metrics;

import com.transferwise.kafka.tkms.api.ShardPartition;
import java.time.Instant;

public interface IMetricsTemplate {

  void recordProxyMessageSendSuccess(ShardPartition shardPartition, String topic, Instant insertTime);

  void recordProxyMessageSendFailure(ShardPartition shardPartition, String topic);

  void recordMessageRegistering(String topic, ShardPartition shardPartition);

  void recordDaoMessageInsert(ShardPartition shardPartition);

  void recordDaoMessagesDeletion(ShardPartition shardPartition, int batchSize);

  void recordProxyPoll(ShardPartition shardPartition, int recordsCount, long startNanotTime);

  void recordDaoPollFirstResult(ShardPartition shardPartition, long startNanoTime);

  void recordDaoPollAllResults(ShardPartition shardPartition, int recordsCount, long startNanoTime);

  void recordDaoPollGetConnection(ShardPartition shardPartition, long startNanoTime);

  void recordProxyCycle(ShardPartition shardPartition, long cycleStartNanoTime);

  void recordProxyKafkaMessagesSend(ShardPartition shardPartition, long startNanoTime);

  void recordProxyMessagesDeletion(ShardPartition shardPartition, long startNanoTime);

  void registerLibrary();

  void recordStoredMessageParsing(ShardPartition shardPartition, long messageParsingStartNanoTime);
}
