package com.transferwise.kafka.tkms.config;

import com.transferwise.common.baseutils.validation.LegacyResolvedValue;
import com.transferwise.common.baseutils.validation.ResolvedValue;
import com.transferwise.kafka.tkms.CompressionAlgorithm;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.beans.factory.InitializingBean;

@Data
@Accessors(chain = true)
public class TkmsProperties implements InitializingBean {

  @Override
  public void afterPropertiesSet() {
    TkmsShardPartition.init(this);
  }

  /**
   * Allows to set notification level or even block the startup/execution, for different problems the library is detecting.
   *
   * <p>The set of keys is described with {@link NotificationType} enum below.
   *
   * <p>The level has to be one from {@link NotificationLevel} enum below.
   */
  private Map<NotificationType, NotificationLevel> notificationLevels = new HashMap<>();

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
  @jakarta.validation.constraints.Positive
  private int partitionsCount = 1;
  /**
   * How many shards there will be.
   *
   * <p>By default we will create 2 shards, even when most services need only one. The idea is to force engineers
   * to have one more shard and table already available, so during a latency-specific incident they have something to quickly fall-over to.
   */
  @Positive
  @jakarta.validation.constraints.Positive
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
  @jakarta.validation.constraints.NotBlank
  @ResolvedValue
  @LegacyResolvedValue
  private String tableBaseName = "outgoing_message";

  /**
   * Uniquely identifying a service in Zookeeper cluster.
   *
   * <p>Used to determine service-cluster wide lock names for Kafka proxies.
   */
  @ResolvedValue
  @LegacyResolvedValue
  private String groupId;
  /**
   * How many messages is Kafka proxy polling from a database at once.
   *
   * <p>Should not be any need to change it.
   *
   * <p>Check `com.transferwise.kafka.tkms.dao.TkmsDao#batchSizes` for most optimal values.
   */
  @Positive
  @jakarta.validation.constraints.Positive
  private int pollerBatchSize = 1024;

  /**
   * Specifies the parameters counts used when executing messages deletions queries, right after successfully sending batch of messages out.
   *
   * <p>You may want/need to reduce the maximum batch sizes, in the case your database tries to execute queries in a very inefficent way. E.g. doing
   * sequential scans on table containing 1 million messages.
   */
  @NotEmpty
  @jakarta.validation.constraints.NotEmpty
  private List<Integer> deleteBatchSizes = List.of(1024, 256, 64, 16, 4, 1);
  /**
   * On batch messages registration, how large database batch size we are using for inserting those messages into the database.
   *
   * <p>Should not be any need to change it.
   */
  @Positive
  @jakarta.validation.constraints.Positive
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
  @jakarta.validation.constraints.NotNull
  private Duration pollingInterval = Duration.ofMillis(25);
  /**
   * How much do we wait on error.
   *
   * <p>For example, when Kafka cluster dies, we don't want to start spamming errors more than 1 time per 2 seconds.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Duration pauseTimeOnErrors = Duration.ofSeconds(2);
  /**
   * Which database engine do we use.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private DatabaseDialect databaseDialect = DatabaseDialect.MARIADB;
  /**
   * How long can one Kafka proxy do it's work, before giving away the leader lock.
   *
   * <p>The idea here is to create a random distribution around service nodes for shard-partition Kafka proxies.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Duration proxyTimeToLive = Duration.ofHours(1);

  /**
   * Safety net for validating message sizes before registering them with tw-tkms.
   *
   * <p>Be extra careful here by validating what is the corresponding value on the Kafka server side.
   */
  @Positive
  @jakarta.validation.constraints.Positive
  private int maximumMessageBytes = 10485760;

  /**
   * Minimum polling interval, overrides all other relevant settings.
   *
   * <p>Can be useful in development environments, where environment owner wants to restrict resource usages globally.
   */
  private Duration minPollingInterval;

  /**
   * How long to wait for the `TkmsStorageToKafkaProxy` to stop, before giving up and logging a timeout error.
   *
   * <p>This could need to be increased to make graceful shutdown noise-free.
   * For example, in case Tkms is configured to use really large batches and/or database is slow.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Duration proxyStopTimeout = Duration.ofSeconds(15);

  /**
   * List topics used by the lib.
   *
   * <p>It is not mandatory, but it allows to do some pre validation and prevent the service starting when something is wrong.
   *
   * <p>Also, so we can warm up their metadata, avoiding elevated latencies at the start of the service.
   */
  @ResolvedValue
  @LegacyResolvedValue
  private List<String> topics = new ArrayList<>();

  @Valid
  @jakarta.validation.Valid
  private EarliestVisibleMessages earliestVisibleMessages = new EarliestVisibleMessages();

  /**
   * Additional or overridden properties for kafka consumers.
   *
   * <p>The default properties are currently set in {@link TkmsKafkaProducerProvider#getKafkaProducer(int)}.
   */
  @ResolvedValue
  @LegacyResolvedValue
  private Map<String, String> kafka = new HashMap<>();

  /**
   * Every shard can override settings from default configuration.
   */
  private Map<Integer, ShardProperties> shards = new HashMap<>();

  /**
   * When enabled, the messages are not immediately written to the database, but collected in memory and written to database just before commit.
   *
   * <p>Allows to reduce transactions latency in case multiple individual messages are registered over the course of that transaction.
   * The latency is reduced by batch inserting all the collected messages in the pre-commit hook.
   *
   * <p>The tradeoff is that higher application memory is required for transactions sending out huge number or/and huge messages.
   * However, in practical applications, large transactions should be avoided anyway.
   *
   * <p>May default to true for Postgres in upcoming versions. Or even default to true in all situations.
   */
  private boolean deferMessageRegistrationUntilCommit = false;

  @Valid
  @jakarta.validation.Valid
  private Compression compression = new Compression();

  @Valid
  @jakarta.validation.Valid
  private Environment environment = new Environment();

  @Valid
  @jakarta.validation.Valid
  private Monitoring monitoring = new Monitoring();

  /**
   * Validation requires quite specific privileges in database. Some teams may need to turn it off.
   */
  private boolean tableStatsValidationEnabled = true;

  @Valid
  @jakarta.validation.Valid
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Internals internals = new Internals();

  @Valid
  @jakarta.validation.Valid
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Mdc mdc = new Mdc();

  @Data
  @Accessors(chain = true)
  public static class Mdc {

    @NotBlank
    @jakarta.validation.constraints.NotBlank
    @ResolvedValue
    @LegacyResolvedValue
    private String shardKey = "tkmsShard";
    @NotBlank
    @jakarta.validation.constraints.NotBlank
    @ResolvedValue
    @LegacyResolvedValue
    private String partitionKey = "tkmsPartition";
  }

  @Data
  @Accessors(chain = true)
  public static class ShardProperties {

    @ResolvedValue
    @LegacyResolvedValue
    private String tableBaseName;
    private DatabaseDialect databaseDialect;
    private Integer partitionsCount;
    private Integer pollerBatchSize;
    private Duration pollingInterval;
    private Duration pauseTimeOnErrors;
    private Integer insertBatchSize;
    private Duration proxyStopTimeout;
    private boolean compressionOverridden;
    private Boolean deferRegisteredMessagesUntilCommit;
    @Valid
    @jakarta.validation.Valid
    private Compression compression = new Compression();
    @Valid
    @jakarta.validation.Valid
    private EarliestVisibleMessages earliestVisibleMessages;
    private List<Integer> deleteBatchSizes;
    private Map<NotificationType, NotificationLevel> notificationLevels = new HashMap<>();

    @ResolvedValue
    @LegacyResolvedValue
    private Map<String, String> kafka = new HashMap<>();
  }

  public Duration getProxyStopTimeout(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getProxyStopTimeout() != null) {
      return shardProperties.getProxyStopTimeout();
    }
    return proxyStopTimeout;
  }

  public String getTableBaseName(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getTableBaseName() != null) {
      return shardProperties.getTableBaseName();
    }
    return tableBaseName;
  }

  public DatabaseDialect getDatabaseDialect(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getDatabaseDialect() != null) {
      return shardProperties.getDatabaseDialect();
    }
    return databaseDialect;
  }

  public int getPartitionsCount(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getPartitionsCount() != null) {
      return shardProperties.getPartitionsCount();
    }
    return partitionsCount;
  }

  public int getPollerBatchSize(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getPollerBatchSize() != null) {
      return shardProperties.getPollerBatchSize();
    }
    return pollerBatchSize;
  }

  public Duration getPollingInterval(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getPollingInterval() != null) {
      return shardProperties.getPollingInterval();
    }
    return pollingInterval;
  }

  public boolean deferMessageRegistrationUntilCommit(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getDeferRegisteredMessagesUntilCommit() != null) {
      return shardProperties.getDeferRegisteredMessagesUntilCommit();
    }
    return deferMessageRegistrationUntilCommit;
  }

  public Duration getPauseTimeOnErrors(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getPauseTimeOnErrors() != null) {
      return shardProperties.getPauseTimeOnErrors();
    }
    return pauseTimeOnErrors;
  }

  public int getInsertBatchSize(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getInsertBatchSize() != null) {
      return shardProperties.getInsertBatchSize();
    }
    return insertBatchSize;
  }

  public Compression getCompression(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.isCompressionOverridden()) {
      return shardProperties.getCompression();
    }
    return compression;
  }

  public EarliestVisibleMessages getEarliestVisibleMessages(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getEarliestVisibleMessages() != null) {
      return shardProperties.getEarliestVisibleMessages();
    }
    return earliestVisibleMessages;
  }

  public List<Integer> getDeleteBatchSizes(int shard) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.deleteBatchSizes != null && !shardProperties.deleteBatchSizes.isEmpty()) {
      return shardProperties.deleteBatchSizes;
    }

    return deleteBatchSizes;
  }

  public NotificationLevel getNotificationLevels(int shard, NotificationType type) {
    var shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.notificationLevels.get(type) != null) {
      return shardProperties.notificationLevels.get(type);
    }
    return notificationLevels.get(type);
  }

  public enum DatabaseDialect {
    POSTGRES,
    MARIADB
  }

  @Data
  @Accessors(chain = true)
  public static class Compression {

    private CompressionAlgorithm algorithm = CompressionAlgorithm.GZIP;

    @Positive
    @jakarta.validation.constraints.Positive
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

    /**
     * Marks that previous deployment version in a specific environment (e.g. production) is at least this.
     *
     * <p>Used for controlling migration path for breaking changes.
     *
     * <p>Does not need to get updated with every upgrade of this library.
     *
     * <p>NB! Never set it higher that you have in a specific environment.
     */
    @ResolvedValue
    @LegacyResolvedValue
    private String previousVersionAtLeast;
  }

  @Data
  @Accessors(chain = true)
  public static class EarliestVisibleMessages {

    private boolean enabled = false;

    @ResolvedValue
    @LegacyResolvedValue
    private String tableName = "tw_tkms_earliest_visible_messages";

    private Duration lookBackPeriod = Duration.ofMinutes(5);

    /**
     * Interval, after which we will poll all records, even when earliest message system has calculated a look-back id.
     *
     * <p>This is for situations where an application has a risk of having long-running transactions, which register messages over its duration.
     * Those messages could get auto incremented id, which is too "old" for the proxy component to see.
     *
     * <p>Notice however, that the order of the messages may change, when the proxy actually finds and forwards those messages with too "old" ids.
     *
     * <p>In case this is set, the first poll without id limit will be done when `StorageToKafkaProxy` acquires a lock.
     * I.e. when a new node starts, or the same nodes starts again, to proxy messages from the database to the Kafka.
     *
     * <p>In most cases you want to set this higher than `proxyTimeToLive`, so it will happen only once per proxy lifecycle (default up to 1 hour).
     */
    private Duration pollAllInterval = null;
  }

  @Data
  @Accessors(chain = true)
  public static class Monitoring {

    private Duration interval = Duration.ofSeconds(30);
    private Duration startDelay = Duration.ofSeconds(30);

    private Duration leftOverMessagesCheckInterval = Duration.ofHours(1);
    private Duration leftOverMessagesCheckStartDelay = Duration.ofHours(1);
  }

  /**
   * Internal toggles for fine-tuning and debugging/testing reasons.
   *
   * <p>Do not touch!
   */
  @Data
  @Accessors(chain = true)
  public static class Internals {

    private int assertionLevel = 0;
  }

  public enum NotificationLevel {
    IGNORE,
    INFO,
    WARN,
    ERROR,
    BLOCK
  }

  /*
    Basically similar idea, what Spotbugs/Checkstyle use to "hide" unwanted warnings.
   */
  public enum NotificationType {

    INDEX_HINTS_NOT_AVAILABLE,
    TABLE_STATS_NOT_FIXED,
    ENGINE_INDEPENDENT_TABLE_STATS_NOT_FIXED,
    INDEX_STATS_NOT_FIXED,
    TABLE_INDEX_STATS_CHECK_ERROR,
    TOO_MANY_DELETE_BATCHES,
    EARLIEST_MESSAGES_SYSTEM_DISABLED,
    ENGINE_INDEPENDENT_STATS_NOT_ENABLED,
    NO_ACTIVE_TRANSACTION
  }
}
