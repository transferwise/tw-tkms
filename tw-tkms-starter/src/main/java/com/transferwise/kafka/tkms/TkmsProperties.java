package com.transferwise.kafka.tkms;

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
  @Positive
  private int shardsCount = 1;

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
