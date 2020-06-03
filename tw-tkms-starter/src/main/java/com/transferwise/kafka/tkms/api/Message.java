package com.transferwise.kafka.tkms.api;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Instant;
import java.util.List;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;
import javax.validation.constraints.Size;
import lombok.Data;
import lombok.Singular;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Message {

  @NotBlank
  private String topic;
  private Instant timestamp;
  @PositiveOrZero
  private Integer partition;
  @Size(min = 1)
  private String key;
  @SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
  private byte[] value;
  @Singular
  private List<Header> headers;

  @PositiveOrZero
  private Integer shard;

  @Data
  @Accessors(chain = true)
  public static class Header {

    @NotBlank
    public String key;
    @NotNull
    public byte[] value;
  }
}
