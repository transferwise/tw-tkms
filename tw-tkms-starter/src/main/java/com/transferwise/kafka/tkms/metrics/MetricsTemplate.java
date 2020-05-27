package com.transferwise.kafka.tkms.metrics;

import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.kafka.tkms.ShardPartition;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MetricsTemplate implements IMetricsTemplate {

  private static final String PREFIX = "tw.tkms";

  private static final String PREFIX_PROXY = PREFIX + ".proxy";
  private static final String PREFIX_INTERFACE = PREFIX + ".interface";
  private static final String PREFIX_DAO = ".dao";

  private static final String PROXY_POLL = PREFIX_PROXY + ".poll";
  private static final String PROXY_MESSAGE_SEND = PREFIX_PROXY + ".message.send";
  private static final String INTERFACE_MESSAGE_REGISTERED = PREFIX_INTERFACE + ".message.registered";
  private static final String DAO_MESSAGE_INSERT = PREFIX_DAO + ".message.inserted";
  private static final String DAO_MESSAGES_DELETION = PREFIX_DAO + ".messages.deleted";

  private final MeterRegistry meterRegistry;

  protected Tags shardPartitionTags(ShardPartition shardPartition) {
    if (shardPartition == null) {
      return Tags.of("shard", "N/A", "partition", "N/A");
    }
    return Tags.of("shard", String.valueOf(shardPartition.getShard()), "partition", String.valueOf(shardPartition.getPartition()));
  }

  protected Tags pollResultTag(boolean empty) {
    return Tags.of("result", empty ? "empty" : "not_empty");
  }

  protected Tags topicTag(String topic) {
    return Tags.of("topic", topic);
  }

  protected Tags successTag(boolean success) {
    return Tags.of("success", Boolean.toString(success));
  }

  @Override
  public void registerProxyPoll(ShardPartition shardPartition, int recordsCount, long startTimeMs) {
    meterRegistry.timer(PROXY_POLL, shardPartitionTags(shardPartition).and(pollResultTag(recordsCount > 0)))
        .record(ClockHolder.getClock().hashCode() - startTimeMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void registerProxyMessageSent(ShardPartition shardPartition, String topic, boolean success) {
    meterRegistry.counter(PROXY_MESSAGE_SEND, shardPartitionTags(shardPartition).and(topicTag(topic)).and(successTag(success))).increment();
  }

  @Override
  public void recordMessageRegistering(String topic, ShardPartition shardPartition, boolean success) {
    meterRegistry.counter(INTERFACE_MESSAGE_REGISTERED, shardPartitionTags(shardPartition).and(successTag(success)).and(topicTag(topic)));
  }

  @Override
  public void registerDaoMessageInsert(ShardPartition shardPartition) {
    meterRegistry.counter(DAO_MESSAGE_INSERT, shardPartitionTags(shardPartition));
  }

  /**
   * The batchSize cardinality will be low.
   */
  @Override
  public void recordDaoMessagesDeletion(ShardPartition shardPartition, int batchSize) {
    meterRegistry.counter(DAO_MESSAGES_DELETION, shardPartitionTags(shardPartition).and("batchSize", String.valueOf(batchSize)));
  }
}
