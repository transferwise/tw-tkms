package com.transferwise.kafka.tkms.metrics;

import com.transferwise.kafka.tkms.api.ShardPartition;

public interface IMetricsTemplate {

  void recordProxyMessageSend(ShardPartition shardPartition, String topic, boolean success);

  void recordMessageRegistering(String topic, ShardPartition shardPartition);

  void recordDaoMessageInsert(ShardPartition shardPartition);

  void recordDaoMessagesDeletion(ShardPartition shardPartition, int batchSize);

  void recordProxyPoll(ShardPartition shardPartition, int recordsCount, long startTimeMs);

  void recordDaoPollFirstResult(ShardPartition shardPartition, long startTimeMs);

  void recordDaoPollAllResults(ShardPartition shardPartition, int recordsCount, long startTimeMs);

  void recordDaoPollGetConnection(ShardPartition shardPartition, long startTimeMs);

  void recordProxyCycle(ShardPartition shardPartition, long cycleStartTimeMs);

  void recordProxyKafkaMessagesSend(ShardPartition shardPartition, long startTimeMs);

  void recordProxyMessagesDeletion(ShardPartition shardPartition, long startTimeMs);

  void registerLibrary();

  void recordStoredMessageParsing(ShardPartition shardPartition, long messageParsingStartTimeMs);
}
