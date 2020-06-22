package com.transferwise.kafka.tkms.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TkmsProperties {

  @Positive
  private int partitionsCount = 1;
  /**
   * Let's force engineers to think about the latency concerns by already introducing 2 shards.
   */
  @Positive
  private int shardsCount = 2;

  private int defaultShard = 0;
  /**
   * Table names are derived by {@code <tableBaseName><shardIndex><partitionIndex>}.
   *
   * <p>For example outgoing_message_0_2;
   */
  @NotBlank
  private String tableBaseName = "outgoing_message";
  private String groupId;
  @Positive
  private int pollerBatchSize = 1000;
  @Positive
  private int insertBatchSize = 1000;
  @NotNull
  private Duration desiredLatency = Duration.ofMillis(5);
  @NotNull
  private Duration pauseTimeOnErrors = Duration.ofSeconds(2);
  @NotNull
  private DatabaseDialect databaseDialect = DatabaseDialect.MYSQL;
  @NotNull
  private Duration proxyTimeToLive = Duration.ofMinutes(10);

  /**
   * List topics used by the lib.
   *
   * <p>It is not mandatory, but it allows to do some pre validation and prevent the service starting when something is wrong.
   *
   * <p>Also, so we can warm up their metadata, avoiding elevated latencies at the start of the service.
   */
  private List<String> topics = new ArrayList<>();

  private Map<String, String> kafka = new HashMap<>();

  private Map<Integer, ShardProperties> shards = new HashMap<>();

  @Data
  @Accessors(chain = true)
  public static class ShardProperties {

    private Integer partitionsCount;
    private Integer pollerBatchSize;
    private Duration desiredLatency;
    private Duration pauseTimeOnErrors;
    private Integer insertBatchSize;

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

  public Duration getDesiredLatency(int shard) {
    ShardProperties shardProperties = shards.get(shard);
    if (shardProperties != null && shardProperties.getDesiredLatency() != null) {
      return shardProperties.getDesiredLatency();
    }
    return desiredLatency;
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

  public enum DatabaseDialect {
    POSTGRES,
    MYSQL
  }
}
