package com.transferwise.kafka.tkms.config;

import com.transferwise.kafka.tkms.CompressionAlgorithm;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TkmsProperties {

  @PostConstruct
  public void init() {
    TkmsShardPartition.init(this);
  }

  /**
   * Provides more metrics at performance penalty.
   */
  private boolean debugEnabled;

  /**
   * The default number of partitions in a shard.
   *
   * <p>You can increase it to increase the throughput or reduce the latency.
   *
   * <p>But in most cases you don't need that - less tables will make it simpler to troubleshoot and correct issues.
   *
   * <p>Every partition will need a separate table to be created.
   *
   * <p>Messages will be saved into partition based on provided partition key or Kafka partition number.
   * If those are not provided, a simple random spread will be used.
   *
   * <p>Do not confuse Tkms partitions with Kafka partitions. For example, message can go to Tkms partition #0, which will be sent to
   * Kafka partition #5.
   */
  @Positive
  private int partitionsCount = 1;
  /**
   * How many shards there will be.
   *
   * <p>By default we will create 2 shards, even when most services need only one. The idea is to force engineers
   * to have one more shard and table already available, so during a latency-specific incident they have something to quickly fall-over to.
   */
  @Positive
  private int shardsCount = 2;

  /**
   * A shard used for messages not specifying shard explicitly.
   */
  private int defaultShard = 0;

  /**
   * Table names are derived by {@code <tableBaseName>_<shardIndex>_<partitionIndex>}.
   *
   * <p>For example {@code outgoing_message_0_2}.
   *
   * <p>Make sure you have all those tables available and correctly configured - index statistics, auto vacuum and analyze settings.
   */
  @NotBlank
  private String tableBaseName = "outgoing_message";

  /**
   * Uniquely identifying a service in Zookeeper cluster.
   *
   * <p>Used to determine service-cluster wide lock names for Kafka proxies.
   */
  private String groupId;
  /**
   * How many messages is Kafka proxy polling from a database at once.
   *
   * <p>Should not be any need to change it.
   *
   * <p>Check `com.transferwise.kafka.tkms.dao.TkmsDao#batchSizes` for most optimal values.
   */
  @Positive
  private int pollerBatchSize = 1024;
  /**
   * On batch messages registration, how large database batch size we are using for inserting those messages into the database.
   *
   * <p>Should not be any need to change it.
   */
  @Positive
  private int insertBatchSize = 1024;
  /**
   * How much do we wait, when the last poll did not find any messages in the database.
   *
   * <p>Tradeoff between low latency and QPS. Due to increased QPS, very low values could actually increase the latency.
   *
   * <p>Probably biggest consideration here is how many QPS you would tolerate.
   * 25ms means up to 40 queries per second per shard-partition. At the same time those empty queries will be very cheap.
   */
  @NotNull
  private Duration pollingInterval = Duration.ofMillis(25);
  /**
   * How much do we wait on error.
   *
   * <p>For example, when Kafka cluster dies, we don't want to start spamming errors more than 1 time per 2 seconds.
   */
  @NotNull
  private Duration pauseTimeOnErrors = Duration.ofSeconds(2);
  /**
   * Which database engine do we use.
   */
  @NotNull
  private DatabaseDialect databaseDialect = DatabaseDialect.MYSQL;
  /**
   * How long can one Kafka proxy do it's work, before giving away the leader lock.
   *
   * <p>The idea here is to create a random distribution around service nodes for shard-partition Kafka proxies.
   */
  @NotNull
  private Duration proxyTimeToLive = Duration.ofMinutes(10);

  /**
   * Is message compression enabled.
   *
   * <p>By default the message compression is enabled as we are optimizing for network and database IO.
   * Only conceivable downside is that there will be no meaningful way for DBAs to search specific messages in the database, But on the other hand,
   * there should be no need for that, those messages are just so short-living in the storage anyway.
   */
  private boolean useCompression = true;

  /**
   * Safety net for validating message sizes before registering them with tw-tkms.
   *
   * <p>Be extra careful here by validating what is the corresponding value on the Kafka server side.
   */
  private int maximumMessageBytes = 10485760;

  /**
   * List topics used by the lib.
   *
   * <p>It is not mandatory, but it allows to do some pre validation and prevent the service starting when something is wrong.
   *
   * <p>Also, so we can warm up their metadata, avoiding elevated latencies at the start of the service.
   */
  private List<String> topics = new ArrayList<>();

  /**
   * Additional or overridden properties for kafka consumers.
   *
   * <p>The default properties are currently set in {@link TkmsKafkaProducerProvider#getKafkaProducer(int)}.
   */
  private Map<String, String> kafka = new HashMap<>();

  /**
   * Every shard can override settings from default configuration.
   */
  private Map<Integer, ShardProperties> shards = new HashMap<>();

  private Compression compression = new Compression();

  private Environment environment = new Environment();

  @Data
  @Accessors(chain = true)
  public static class ShardProperties {

    private Integer partitionsCount;
    private Integer pollerBatchSize;
    private Duration pollingInterval;
    private Duration pauseTimeOnErrors;
    private Integer insertBatchSize;
    private boolean compressionOverridden;
    private Compression compression = new Compression();

    private Map<String, String> kafka = new HashMap<>();
  }

  public int getPartitionsCount(int shard) {
    ShardProperties shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getPartitionsCount() != null) {
      return shardProperties.getPartitionsCount();
    }
    return partitionsCount;
  }

  public int getPollerBatchSize(int shard) {
    ShardProperties shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getPollerBatchSize() != null) {
      return shardProperties.getPollerBatchSize();
    }
    return pollerBatchSize;
  }

  public Duration getPollingInterval(int shard) {
    ShardProperties shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getPollingInterval() != null) {
      return shardProperties.getPollingInterval();
    }
    return pollingInterval;
  }

  public Duration getPauseTimeOnErrors(int shard) {
    ShardProperties shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getPauseTimeOnErrors() != null) {
      return shardProperties.getPauseTimeOnErrors();
    }
    return pauseTimeOnErrors;
  }

  public int getInsertBatchSize(int shard) {
    ShardProperties shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getInsertBatchSize() != null) {
      return shardProperties.getInsertBatchSize();
    }
    return insertBatchSize;
  }

  public Compression getCompression(int shard) {
    ShardProperties shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.isCompressionOverridden()) {
      return shardProperties.getCompression();
    }
    return compression;
  }

  public enum DatabaseDialect {
    POSTGRES,
    MYSQL
  }

  @Data
  @Accessors(chain = true)
  public static class Compression {

    private CompressionAlgorithm algorithm = CompressionAlgorithm.GZIP;

    private Integer blockSize;

    /**
     * Minimum message size, to apply compression.
     *
     * <p>Approximate message size is considered.
     */
    private int minSize = 128;

    private Integer level;
  }

  @Data
  @Accessors(chain = true)
  public static class Environment {

    private String previousVersion;
  }
}
