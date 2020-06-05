package com.transferwise.kafka.tkms.config;

import java.time.Duration;
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
  @NotBlank
  private String groupId;
  @Positive
  private int pollerBatchSize = 1000;
  @NotNull
  private Duration desiredLatency = Duration.ofMillis(5);
  @NotNull
  private Duration pauseTimeOnErrors = Duration.ofSeconds(5);
  @NotNull
  private DatabaseDialect databaseDialect = DatabaseDialect.MYSQL;

  public enum DatabaseDialect {
    POSTGRES,
    MYSQL
  }
}
