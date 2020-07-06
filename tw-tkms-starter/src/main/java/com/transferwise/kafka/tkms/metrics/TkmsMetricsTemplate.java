package com.transferwise.kafka.tkms.metrics;

import com.transferwise.common.context.TwContext;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TkmsMetricsTemplate implements ITkmsMetricsTemplate {

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
  public static final String DAO_POLL_ALL_RESULTS = PREFIX_DAO + ".poll.all.results";
  public static final String DAO_POLL_ALL_RESULTS_COUNT = PREFIX_DAO + ".poll.all.results.count";
  public static final String STORED_MESSAGE_PARSING = PREFIX + ".stored.message.parsing";
  public static final String MESSAGE_INSERT_TO_ACK = PREFIX + ".message.insert.to.ack";

  public static final String TAG_EP_NAME = "epName";
  public static final String TAG_EP_GROUP = "epGroup";
  public static final String TAG_EP_OWNER = "epOwner";

  private final MeterRegistry meterRegistry;

  @PostConstruct
  public void init() {
    Map<String, long[]> slas = new HashMap<>();
    long[] defaultSlas = new long[]{1, 5, 25, 125, 625, 3125};
    slas.put(PROXY_POLL, defaultSlas);
    slas.put(PROXY_CYCLE, defaultSlas);
    slas.put(DAO_POLL_FIRST_RESULT, defaultSlas);
    slas.put(DAO_POLL_ALL_RESULTS, defaultSlas);
    slas.put(DAO_POLL_GET_CONNECTION, defaultSlas);
    slas.put(PROXY_KAFKA_MESSAGES_SEND, defaultSlas);
    slas.put(PROXY_MESSAGES_DELETION, defaultSlas);
    slas.put(STORED_MESSAGE_PARSING, defaultSlas);

    slas.put(MESSAGE_INSERT_TO_ACK, new long[]{1, 5, 25, 125, 625, 3125 * 5});

    meterRegistry.config().meterFilter(new MeterFilter() {
      @Override
      public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
        long[] slaConfigValues = slas.get(id.getName());
        if (slaConfigValues != null) {
          long[] slaValues = Arrays.copyOf(slaConfigValues, slaConfigValues.length);
          for (int i = 0; i < slaValues.length; i++) {
            slaValues[i] = slaValues[i] * 1_000_000L;
          }
          return DistributionStatisticConfig.builder()
              .percentilesHistogram(false)
              .sla(slaValues)
              .build()
              .merge(config);
        }
        return config;
      }
    });
  }

  @Override
  public void recordProxyPoll(TkmsShardPartition shardPartition, int recordsCount, long startNanoTime) {
    meterRegistry.timer(PROXY_POLL, shardPartitionTags(shardPartition).and(pollResultTag(recordsCount > 0)))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordProxyMessageSendSuccess(TkmsShardPartition shardPartition, String topic, Instant insertTime) {
    meterRegistry.counter(PROXY_MESSAGE_SEND, shardPartitionTags(shardPartition).and(topicTag(topic)).and(successTag(true))).increment();

    if (insertTime != null) {
      meterRegistry.timer(MESSAGE_INSERT_TO_ACK, shardPartitionTags(shardPartition).and(topicTag(topic)))
          .record(System.currentTimeMillis() - insertTime.toEpochMilli(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void recordProxyMessageSendFailure(TkmsShardPartition shardPartition, String topic) {
    meterRegistry.counter(PROXY_MESSAGE_SEND, shardPartitionTags(shardPartition).and(topicTag(topic)).and(successTag(false))).increment();
  }


  @Override
  public void recordMessageRegistering(String topic, TkmsShardPartition shardPartition) {
    meterRegistry
        .counter(INTERFACE_MESSAGE_REGISTERED, entryPointTags().and(shardPartitionTags(shardPartition)).and(topicTag(topic)))
        .increment();
  }

  @Override
  public void recordDaoMessageInsert(TkmsShardPartition shardPartition) {
    meterRegistry.counter(DAO_MESSAGE_INSERT, entryPointTags().and(shardPartitionTags(shardPartition))).increment();
  }

  @Override
  public void recordDaoPollFirstResult(TkmsShardPartition shardPartition, long startNanoTime) {
    meterRegistry.timer(DAO_POLL_FIRST_RESULT, shardPartitionTags(shardPartition))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordDaoPollAllResults(TkmsShardPartition shardPartition, int recordsCount, long startNanoTime) {
    meterRegistry.timer(DAO_POLL_ALL_RESULTS, shardPartitionTags(shardPartition))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
    meterRegistry.summary(DAO_POLL_ALL_RESULTS_COUNT, shardPartitionTags(shardPartition)).record(recordsCount);
  }

  @Override
  public void recordDaoPollGetConnection(TkmsShardPartition shardPartition, long startNanoTime) {
    meterRegistry.timer(DAO_POLL_GET_CONNECTION, shardPartitionTags(shardPartition))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordProxyCycle(TkmsShardPartition shardPartition, long startNanoTime) {
    meterRegistry.timer(PROXY_CYCLE, shardPartitionTags(shardPartition))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordProxyKafkaMessagesSend(TkmsShardPartition shardPartition, long startNanoTime) {
    meterRegistry.timer(PROXY_KAFKA_MESSAGES_SEND, shardPartitionTags(shardPartition))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordProxyMessagesDeletion(TkmsShardPartition shardPartition, long startNanoTime) {
    meterRegistry.timer(PROXY_MESSAGES_DELETION, shardPartitionTags(shardPartition))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  /**
   * The batchSize cardinality will be low.
   *
   * <p>batchSize tag allows to verify algorithmic correctness for deletions.
   */
  @Override
  public void recordDaoMessagesDeletion(TkmsShardPartition shardPartition, int batchSize) {
    meterRegistry.counter(DAO_MESSAGES_DELETION, shardPartitionTags(shardPartition).and("batchSize", String.valueOf(batchSize)))
        .increment();
  }

  @Override
  public void recordStoredMessageParsing(TkmsShardPartition shardPartition, long messageParsingStartNanoTime) {
    meterRegistry.timer(STORED_MESSAGE_PARSING, shardPartitionTags(shardPartition))
        .record(System.nanoTime() - messageParsingStartNanoTime, TimeUnit.NANOSECONDS);
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

  protected Tags shardPartitionTags(TkmsShardPartition shardPartition) {
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
