package com.transferwise.kafka.tkms.metrics;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.common.baseutils.meters.cache.TagsSet;
import com.transferwise.common.context.TwContext;
import com.transferwise.kafka.tkms.CompressionAlgorithm;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Type;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.PostConstruct;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/*
  A bit over engineering, but:
    - specify tags in ascending order, so micrometer has less work to do.
    - avoid `Tags.and()` to create less objects/arrays.
 */
@RequiredArgsConstructor
@Slf4j
public class TkmsMetricsTemplate implements ITkmsMetricsTemplate {

  public static final String GAUGE_LIBRARY_INFO = "tw_library_info";
  public static final String TIMER_PROXY_POLL = "tw_tkms_proxy_poll";
  public static final String GAUGE_PROXY_POLL_IN_PROGRESS = "tw_tkms_proxy_poll_in_progress";
  public static final String TIMER_PROXY_CYCLE = "tw_tkms_proxy_cycle";
  public static final String TIMER_PROXY_CYCLE_PAUSE = "tw_tkms_proxy_cycle_pause";
  public static final String COUNTER_PROXY_MESSAGE_SEND = "tw_tkms_proxy_message_send";
  public static final String TIMER_PROXY_KAFKA_MESSAGES_SEND = "tw_tkms_proxy_kafka_messages_send";
  public static final String TIMER_PROXY_MESSAGES_DELETION = "tw_tkms_proxy_messages_delete";
  public static final String COUNTER_INTERFACE_MESSAGE_REGISTERED = "tw_tkms_interface_message_registration";
  public static final String COUNTER_DAO_MESSAGE_INSERT = "tw_tkms_dao_message_insert";
  public static final String COUNTER_DAO_MESSAGES_DELETION = "tw_tkms_dao_messages_delete";
  public static final String COUNTER_DAO_MESSAGES_DELETION_BATCHES = "tw_tkms_dao_messages_delete_batches";
  public static final String TIMER_DAO_POLL_FIRST_RESULT = "tw_tkms_dao_poll_first_result";
  public static final String TIMER_DAO_POLL_GET_CONNECTION = "tw_tkms_dao_poll_get_connection";
  public static final String TIMRE_DAO_POLL_ALL_RESULTS = "tw_tkms_dao_poll_all_results";
  public static final String SUMMARY_DAO_POLL_ALL_RESULTS_COUNT = "tw_tkms_dao_poll_all_results_count";
  public static final String COUNTER_DAO_INVALID_GENERATED_KEYS_COUNT = "tw_tkms_dao_insert_invalid_generated_keys_count";
  public static final String GAUGE_DAO_ROWS_IN_TABLE_STATS = "tw_tkms_dao_rows_in_table_stats";
  public static final String GAUGE_DAO_ROWS_IN_ENGINE_INDEPENDENT_TABLE_STATS = "tw_tkms_dao_rows_in_engine_independent_table_stats";
  public static final String GAUGE_DAO_ROWS_IN_INDEX_STATS = "tw_tkms_dao_rows_in_index_stats";
  public static final String GAUGE_DAO_APPROXIMATE_MESSAGES_COUNT = "tw_tkms_dao_approximate_messages_count";
  public static final String TIMER_MESSAGE_INSERT_TO_ACK = "tw_tkms_message_insert_to_ack";
  public static final String SUMMARY_DAO_COMPRESSION_RATIO_ACHIEVED = "tw_tkms_dao_serialization_compression_ratio";
  public static final String COUNTER_DAO_ORIGINAL_SIZE_BYTES = "tw_tkms_dao_serialization_original_size_bytes";
  public static final String COUNTER_DAO_SERIALIZED_SIZE_BYTES = "tw_tkms_dao_serialization_serialized_size_bytes";
  public static final String GAUGE_DAO_EARLIEST_MESSAGE_ID = "tw_tkms_dao_earliest_message_id";
  public static final String COUNTER_DAO_EARLIEST_MESSAGE_ID_COMMIT = "tw_tkms_dao_earliest_message_id_commit";

  public static final Tag NA_SHARD_TAG = Tag.of("shard", "N/A");
  public static final Tag NA_PARTITION_TAG = Tag.of("partition", "N/A");
  public static final Tag TAG_SUCCESS_TRUE = Tag.of("success", "true");
  public static final Tag TAG_SUCCESS_FALSE = Tag.of("success", "false");
  public static final Tag TAG_POLL_RESULT_EMPTY = Tag.of("pollResult", "empty");
  public static final Tag TAG_POLL_RESULTS_NOT_EMPTY = Tag.of("pollResult", "not_empty");

  private final IMeterCache meterCache;
  private final TkmsProperties tkmsProperties;
  
  @PostConstruct
  public void init() {
    Map<String, double[]> slos = new HashMap<>();
    double[] defaultSlos = new double[]{1, 5, 25, 125, 625, 3125, 15625};
    slos.put(TIMER_PROXY_POLL, defaultSlos);
    slos.put(TIMER_PROXY_CYCLE, defaultSlos);
    slos.put(TIMER_PROXY_CYCLE_PAUSE, defaultSlos);
    slos.put(TIMER_DAO_POLL_FIRST_RESULT, defaultSlos);
    slos.put(TIMRE_DAO_POLL_ALL_RESULTS, defaultSlos);
    slos.put(TIMER_DAO_POLL_GET_CONNECTION, defaultSlos);
    slos.put(TIMER_PROXY_KAFKA_MESSAGES_SEND, defaultSlos);
    slos.put(TIMER_PROXY_MESSAGES_DELETION, defaultSlos);
    slos.put(SUMMARY_DAO_POLL_ALL_RESULTS_COUNT, defaultSlos);
    slos.put(TIMER_MESSAGE_INSERT_TO_ACK, new double[]{1, 5, 25, 125, 625, 3125, 15625});
    slos.put(SUMMARY_DAO_COMPRESSION_RATIO_ACHIEVED, new double[]{0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.25, 2, 4});

    meterCache.getMeterRegistry().config().meterFilter(new MeterFilter() {
      @Override
      public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
        double[] sloConfigValues = slos.get(id.getName());
        if (sloConfigValues != null) {
          double[] sloValues = Arrays.copyOf(sloConfigValues, sloConfigValues.length);
          if (id.getType() == Type.TIMER) {
            for (int i = 0; i < sloValues.length; i++) {
              sloValues[i] = sloValues[i] * 1_000_000L;
            }
          }
          return DistributionStatisticConfig.builder()
              .percentilesHistogram(false)
              .serviceLevelObjectives(sloValues)
              .build()
              .merge(config);
        }
        return config;
      }
    });
  }

  @Override
  public void recordProxyPoll(TkmsShardPartition shardPartition, int recordsCount, long startNanoTime) {
    meterCache
        .timer(TIMER_PROXY_POLL, TagsSet.of(
            partitionTag(shardPartition),
            pollResultTag(recordsCount == 0),
            shardTag(shardPartition)))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordProxyMessageSendSuccess(TkmsShardPartition shardPartition, String topic, Instant insertTime) {
    meterCache
        .counter(COUNTER_PROXY_MESSAGE_SEND, TagsSet.of(
            partitionTag(shardPartition),
            shardTag(shardPartition),
            successTag(true),
            topicTag(topic)))
        .increment();

    if (insertTime != null) {
      meterCache
          .timer(TIMER_MESSAGE_INSERT_TO_ACK, TagsSet.of(
              partitionTag(shardPartition),
              shardTag(shardPartition),
              topicTag(topic)))
          .record(System.currentTimeMillis() - insertTime.toEpochMilli(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void recordProxyMessageSendFailure(TkmsShardPartition shardPartition, String topic) {
    meterCache
        .counter(COUNTER_PROXY_MESSAGE_SEND, TagsSet.of(
            partitionTag(shardPartition),
            shardTag(shardPartition),
            successTag(false),
            topicTag(topic)))
        .increment();
  }

  @Override
  public void recordMessageRegistering(String topic, TkmsShardPartition shardPartition) {
    TwContext currentContext = TwContext.current();
    meterCache
        .counter(COUNTER_INTERFACE_MESSAGE_REGISTERED, TagsSet.of(
            entryPointGroupTag(currentContext),
            entryPointNameTag(currentContext),
            entryPointOwnerTag(currentContext),
            partitionTag(shardPartition),
            shardTag(shardPartition),
            topicTag(topic)))
        .increment();
  }

  @Override
  public void recordDaoMessageInsert(TkmsShardPartition shardPartition, String topic) {
    TwContext currentContext = TwContext.current();
    meterCache
        .counter(COUNTER_DAO_MESSAGE_INSERT, TagsSet.of(
            entryPointGroupTag(currentContext),
            entryPointNameTag(currentContext),
            entryPointOwnerTag(currentContext),
            partitionTag(shardPartition),
            shardTag(shardPartition),
            topicTag(topic)))
        .increment();
  }

  @Override
  public void recordDaoPollFirstResult(TkmsShardPartition shardPartition, long startNanoTime) {
    meterCache
        .timer(TIMER_DAO_POLL_FIRST_RESULT, TagsSet.of(
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordDaoPollAllResults(TkmsShardPartition shardPartition, int recordsCount, long startNanoTime) {
    meterCache
        .timer(TIMRE_DAO_POLL_ALL_RESULTS, TagsSet.of(
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
    meterCache
        .summary(SUMMARY_DAO_POLL_ALL_RESULTS_COUNT, TagsSet.of(
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .record(recordsCount);
  }

  @Override
  public void recordDaoPollGetConnection(TkmsShardPartition shardPartition, long startNanoTime) {
    meterCache
        .timer(TIMER_DAO_POLL_GET_CONNECTION, TagsSet.of(
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordProxyCycle(TkmsShardPartition shardPartition, int recordsCount, long startNanoTime) {
    meterCache
        .timer(TIMER_PROXY_CYCLE, TagsSet.of(
            partitionTag(shardPartition),
            pollResultTag(recordsCount == 0),
            shardTag(shardPartition)))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordProxyCyclePause(TkmsShardPartition shardPartition, long durationMs) {
    meterCache
        .timer(TIMER_PROXY_CYCLE_PAUSE, TagsSet.of(
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .record(durationMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void recordProxyKafkaMessagesSend(TkmsShardPartition shardPartition, long startNanoTime) {
    meterCache
        .timer(TIMER_PROXY_KAFKA_MESSAGES_SEND, TagsSet.of(
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordProxyMessagesDeletion(TkmsShardPartition shardPartition, long startNanoTime) {
    meterCache
        .timer(TIMER_PROXY_MESSAGES_DELETION, TagsSet.of(
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .record(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  /**
   * The batchSize cardinality will be low.
   *
   * <p>batchSize tag allows to verify algorithmic correctness for deletions.
   */
  @Override
  public void recordDaoMessagesDeletion(TkmsShardPartition shardPartition, int batchSize) {
    meterCache
        .counter(COUNTER_DAO_MESSAGES_DELETION_BATCHES, TagsSet.of(
            batchSizeTag(batchSize),
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .increment();

    meterCache
        .counter(COUNTER_DAO_MESSAGES_DELETION, TagsSet.of(
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .increment(batchSize);
  }

  @Override
  public void recordMessageSerialization(TkmsShardPartition shardPartition, CompressionAlgorithm algorithm, long originalSizeBytes,
      long serializedSizeBytes) {
    double ratio = (double) originalSizeBytes / serializedSizeBytes;
    meterCache
        .summary(SUMMARY_DAO_COMPRESSION_RATIO_ACHIEVED, TagsSet.of(
            algorithmTag(algorithm),
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .record(ratio);
    meterCache
        .counter(COUNTER_DAO_ORIGINAL_SIZE_BYTES, TagsSet.of(
            algorithmTag(algorithm),
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .increment(originalSizeBytes);
    meterCache
        .counter(COUNTER_DAO_SERIALIZED_SIZE_BYTES, TagsSet.of(
            algorithmTag(algorithm),
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .increment(serializedSizeBytes);
  }

  @Override
  public void recordDaoInvalidGeneratedKeysCount(TkmsShardPartition shardPartition) {
    meterCache
        .counter(COUNTER_DAO_INVALID_GENERATED_KEYS_COUNT, TagsSet.of(
            partitionTag(shardPartition),
            shardTag(shardPartition)))
        .increment();
  }

  @Override
  public void registerLibrary() {
    String version = this.getClass().getPackage().getImplementationVersion();
    if (version == null) {
      version = "Unknown";
    }

    Gauge.builder(GAUGE_LIBRARY_INFO, () -> 1d).tags("version", version, "library", "tw-tkms")
        .description("Provides metadata about the library, for example the version.")
        .register(meterCache.getMeterRegistry());
  }

  @Override
  public Object registerEarliestMessageId(TkmsShardPartition shardPartition, Supplier<Number> supplier) {
    return registerGauge(GAUGE_DAO_EARLIEST_MESSAGE_ID, supplier, shardTag(shardPartition), partitionTag(shardPartition));
  }

  @Override
  public void registerRowsInTableStats(TkmsShardPartition sp, long rowsInTableStats) {
    Gauge.builder(GAUGE_DAO_ROWS_IN_TABLE_STATS, () -> rowsInTableStats).tags(Tags.of(shardTag(sp), partitionTag(sp)))
        .register(meterCache.getMeterRegistry());
  }

  @Override
  public void registerRowsInEngineIndependentTableStats(TkmsShardPartition sp, long rowsInTableStats) {
    Gauge.builder(GAUGE_DAO_ROWS_IN_ENGINE_INDEPENDENT_TABLE_STATS, () -> rowsInTableStats).tags(Tags.of(shardTag(sp), partitionTag(sp)))
        .register(meterCache.getMeterRegistry());
  }

  @Override
  public void registerRowsInIndexStats(TkmsShardPartition sp, long rowsInIndexStats) {
    Gauge.builder(GAUGE_DAO_ROWS_IN_INDEX_STATS, () -> rowsInIndexStats).tags(Tags.of(shardTag(sp), partitionTag(sp)))
        .register(meterCache.getMeterRegistry());
  }

  protected Tag batchSizeTag(int batchSize) {
    return Tag.of("batchSize", String.valueOf(batchSize));
  }

  protected Tag entryPointGroupTag(TwContext twContext) {
    return Tag.of("epGroup", twContext.getGroup());
  }

  protected Tag entryPointNameTag(TwContext twContext) {
    return Tag.of("epName", twContext.getName());
  }

  protected Tag entryPointOwnerTag(TwContext twContext) {
    return Tag.of("epOwner", twContext.getOwner());
  }

  protected Tag shardTag(TkmsShardPartition shardPartition) {
    if (shardPartition == null) {
      return NA_SHARD_TAG;
    }
    return shardPartition.getMicrometerShardTag();
  }

  protected Tag partitionTag(TkmsShardPartition shardPartition) {
    if (shardPartition == null) {
      return NA_PARTITION_TAG;
    }
    return shardPartition.getMicrometerPartitionTag();
  }

  protected Tag pollResultTag(boolean empty) {
    return empty ? TAG_POLL_RESULT_EMPTY : TAG_POLL_RESULTS_NOT_EMPTY;
  }

  protected Tag topicTag(String topic) {
    return Tag.of("topic", topic);
  }

  protected Tag successTag(boolean success) {
    return success ? TAG_SUCCESS_TRUE : TAG_SUCCESS_FALSE;
  }

  protected Tag algorithmTag(CompressionAlgorithm algorithm) {
    return algorithm.getMicrometerTag();
  }

  protected Tag databaseDialectTag(TkmsShardPartition sp){
    return Tag.of("databaseDialect", tkmsProperties.getDatabaseDialect(sp.getShard()).name());
  }
  
  @Override
  public void unregisterMetric(Object rawMetricHandle) {
    if (rawMetricHandle == null) {
      log.error("Algorithm error detected, metricHandle is null.", new Exception("Algorithm error detected, metricHandle is null."));
      return;
    }

    MetricHandle metricHandle = (MetricHandle) rawMetricHandle;

    if (metricHandle.cached) {
      meterCache.removeMeter(metricHandle.getMeter().getId().getName(), metricHandle.getTags());
    }
    meterCache.getMeterRegistry().remove(metricHandle.getMeter());
  }

  @Override
  public Object registerApproximateMessagesCount(TkmsShardPartition sp, Supplier<Number> supplier) {
    return registerGauge(GAUGE_DAO_APPROXIMATE_MESSAGES_COUNT, supplier, shardTag(sp), partitionTag(sp));
  }
  
  @Override
  public Object registerPollingGauge(TkmsShardPartition sp){
    return registerGauge(GAUGE_PROXY_POLL_IN_PROGRESS, ()->1d, shardTag(sp), partitionTag(sp), databaseDialectTag(sp));
  }

  @Override
  public void registerEarliestMessageIdCommit(TkmsShardPartition shardPartition) {
    meterCache.counter(COUNTER_DAO_EARLIEST_MESSAGE_ID_COMMIT, TagsSet.of(shardTag(shardPartition), partitionTag(shardPartition))).increment();
  }

  protected MetricHandle registerGauge(String name, Supplier<Number> supplier, Tag... tags) {
    return new MetricHandle().setMeter(Gauge.builder(name, supplier)
        .tags(Tags.of(tags)).register(meterCache.getMeterRegistry()));
  }

  @Data
  @Accessors(chain = true)
  protected static class MetricHandle {

    private boolean cached;
    private TagsSet tags;
    private Meter meter;

  }
}
