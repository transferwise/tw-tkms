package com.transferwise.kafka.tkms.metrics;

import com.transferwise.common.baseutils.meters.cache.TagsSet;
import com.transferwise.kafka.tkms.CompressionAlgorithm;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import io.micrometer.core.instrument.Meter;
import java.time.Instant;
import java.util.function.Supplier;
import lombok.Data;
import lombok.experimental.Accessors;

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

  void recordMessageSerialization(TkmsShardPartition shardPartition, CompressionAlgorithm algorithm, long originalSizeBytes,
      long serializedSizeBytes);

  void recordDaoInvalidGeneratedKeysCount(TkmsShardPartition shardPartition);

  Object registerEarliestMessageId(TkmsShardPartition shardPartition, Supplier<Number> supplier);

  void deRegisterEarliestMessageId(Object handle);

  void registerRowsInTableStats(TkmsShardPartition sp, long rowsInTableStats);

  void registerRowsInIndexStats(TkmsShardPartition sp, long rowsInIndexStats);
  
  void unregisterMetric(Object rawMetricHandle);

  Object registerApproximateMessagesCount(TkmsShardPartition sp, Supplier<Number> supplier);

  void registerEarliestMessageIdCommit(TkmsShardPartition shardPartition);
}
