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
  private int shardsCount = 1;
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
