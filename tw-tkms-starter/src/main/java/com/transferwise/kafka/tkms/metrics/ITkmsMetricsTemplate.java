package com.transferwise.kafka.tkms.metrics;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.config.TkmsProperties.Compression.Algorithm;
import java.time.Instant;

public interface ITkmsMetricsTemplate {

  void recordProxyMessageSendSuccess(TkmsShardPartition shardPartition, String topic, Instant insertTime);

  void recordProxyMessageSendFailure(TkmsShardPartition shardPartition, String topic);

  void recordMessageRegistering(String topic, TkmsShardPartition shardPartition);

  void recordDaoMessageInsert(TkmsShardPartition shardPartition, String topic);

  void recordDaoMessagesDeletion(TkmsShardPartition shardPartition, int batchSize);

  void recordProxyPoll(TkmsShardPartition shardPartition, int recordsCount, long startNanotTime);

  void recordDaoPollFirstResult(TkmsShardPartition shardPartition, long startNanoTime);

  void recordDaoPollAllResults(TkmsShardPartition shardPartition, int recordsCount, long startNanoTime);

  void recordDaoPollGetConnection(TkmsShardPartition shardPartition, long startNanoTime);

  void recordProxyCycle(TkmsShardPartition shardPartition, int recordsCount, long startNanoTime);

  void recordProxyKafkaMessagesSend(TkmsShardPartition shardPartition, long startNanoTime);

  void recordProxyMessagesDeletion(TkmsShardPartition shardPartition, long startNanoTime);

  void registerLibrary();

  void recordStoredMessageParsing(TkmsShardPartition shardPartition, long messageParsingStartNanoTime);

  void recordMessageCompression(TkmsShardPartition shardPartition, Algorithm algorithm, double ratio);
}
