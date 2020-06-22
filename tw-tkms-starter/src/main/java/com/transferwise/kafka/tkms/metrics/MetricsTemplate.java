package com.transferwise.kafka.tkms.metrics;

import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.common.context.TwContext;
import com.transferwise.kafka.tkms.api.ShardPartition;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MetricsTemplate implements IMetricsTemplate {

  public static final String PREFIX = "tw.tkms";

  public static final String PREFIX_PROXY = PREFIX + ".proxy";
  public static final String PREFIX_INTERFACE = PREFIX + ".interface";
  public static final String PREFIX_DAO = PREFIX + ".dao";

  public static final String METRIC_LIBRARY_INFO = "tw.library.info";
  public static final String PROXY_POLL = PREFIX_PROXY + ".poll";
  public static final String PROXY_CYCLE = PREFIX_PROXY + ".cycle";
  public static final String PROXY_MESSAGE_SEND = PREFIX_PROXY + ".message.send";
  public static final String PROXY_KAFKA_MESSAGES_SEND = PREFIX_PROXY + ".kafka.messages.send";
  public static final String PROXY_MESSAGES_DELETION = PREFIX_PROXY + ".messages.delete";
  public static final String INTERFACE_MESSAGE_REGISTERED = PREFIX_INTERFACE + ".message.registration";
  public static final String DAO_MESSAGE_INSERT = PREFIX_DAO + ".message.insert";
  public static final String DAO_MESSAGES_DELETION = PREFIX_DAO + ".messages.delete";
  public static final String DAO_POLL_FIRST_RESULT = PREFIX_DAO + ".poll.first.result";
  public static final String DAO_POLL_GET_CONNECTION = PREFIX_DAO + ".poll.get.connection";
  public static final String DAO_POLL = PREFIX_DAO + ".poll";

  public static final String TAG_EP_NAME = "epName";
  public static final String TAG_EP_GROUP = "epGroup";
  public static final String TAG_EP_OWNER = "epOwner";

  private final MeterRegistry meterRegistry;

  @Override
  public void recordProxyPoll(ShardPartition shardPartition, int recordsCount, long startTimeMs) {
    meterRegistry
        .timer(PROXY_POLL, shardPartitionTags(shardPartition).and(pollResultTag(recordsCount > 0)))
        .record(ClockHolder.getClock().millis() - startTimeMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void recordProxyMessageSend(ShardPartition shardPartition, String topic, boolean success) {
    meterRegistry.counter(PROXY_MESSAGE_SEND, shardPartitionTags(shardPartition).and(topicTag(topic)).and(successTag(success)))
        .increment();
  }

  @Override
  public void recordMessageRegistering(String topic, ShardPartition shardPartition) {
    meterRegistry
        .counter(INTERFACE_MESSAGE_REGISTERED, entryPointTags().and(shardPartitionTags(shardPartition)).and(topicTag(topic)))
        .increment();
  }

  @Override
  public void recordDaoMessageInsert(ShardPartition shardPartition) {
    meterRegistry.counter(DAO_MESSAGE_INSERT, entryPointTags().and(shardPartitionTags(shardPartition))).increment();
  }

  @Override
  public void recordDaoPollFirstResult(ShardPartition shardPartition, long startTimeMs) {
    meterRegistry.timer(DAO_POLL_FIRST_RESULT, shardPartitionTags(shardPartition))
        .record(ClockHolder.getClock().millis() - startTimeMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void recordDaoPollGetConnection(ShardPartition shardPartition, long startTimeMs) {
    meterRegistry.timer(DAO_POLL_GET_CONNECTION, shardPartitionTags(shardPartition))
        .record(ClockHolder.getClock().millis() - startTimeMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void recordProxyCycle(ShardPartition shardPartition, long startTimeMs) {
    meterRegistry.timer(PROXY_CYCLE, shardPartitionTags(shardPartition))
        .record(ClockHolder.getClock().millis() - startTimeMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void recordProxyKafkaMessagesSend(ShardPartition shardPartition, long startTimeMs) {
    meterRegistry.timer(PROXY_KAFKA_MESSAGES_SEND, shardPartitionTags(shardPartition))
        .record(ClockHolder.getClock().millis() - startTimeMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void recordProxyMessagesDeletion(ShardPartition shardPartition, long startTimeMs) {
    meterRegistry.timer(PROXY_MESSAGES_DELETION, shardPartitionTags(shardPartition))
        .record(ClockHolder.getClock().millis() - startTimeMs, TimeUnit.MILLISECONDS);
  }


  @Override
  public void recordDaoPoll(ShardPartition shardPartition, long startTimeMs) {
    meterRegistry.timer(DAO_POLL, shardPartitionTags(shardPartition))
        .record(ClockHolder.getClock().millis() - startTimeMs, TimeUnit.MILLISECONDS);
  }


  /**
   * The batchSize cardinality will be low.
   *
   * <p>batchSize tag allows to verify algorithmic correctness for deletions.
   */
  @Override
  public void recordDaoMessagesDeletion(ShardPartition shardPartition, int batchSize) {
    meterRegistry.counter(DAO_MESSAGES_DELETION, shardPartitionTags(shardPartition).and("batchSize", String.valueOf(batchSize)))
        .increment();
  }

  @Override
  public void registerLibrary() {
    String version = this.getClass().getPackage().getImplementationVersion();
    if (version == null) {
      version = "Unknown";
    }

    Gauge.builder(METRIC_LIBRARY_INFO, () -> 1d).tags("version", version, "library", "tw-tkms")
        .description("Provides metadata about the library, for example the version.")
        .register(meterRegistry);
  }


  protected Tags entryPointTags() {
    TwContext twContext = TwContext.current();
    return Tags.of(TAG_EP_GROUP, twContext.getGroup(), TAG_EP_NAME, twContext.getName(), TAG_EP_OWNER, twContext.getOwner());
  }

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
}
