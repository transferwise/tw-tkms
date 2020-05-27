package com.transferwise.kafka.tkms.metrics;

import com.transferwise.kafka.tkms.ShardPartition;

public interface IMetricsTemplate {

  void registerProxyMessageSent(ShardPartition shardPartition, String topic, boolean success);

  void recordMessageRegistering(String topic, ShardPartition shardPartition, boolean success);

  void registerDaoMessageInsert(ShardPartition shardPartition);

  void recordDaoMessagesDeletion(ShardPartition shardPartition, int batchSize);

  void registerProxyPoll(ShardPartition shardPartition, int recordsCount, long startTimeMs);
}
