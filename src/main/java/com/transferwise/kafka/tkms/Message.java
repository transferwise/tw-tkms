package com.transferwise.kafka.tkms;

import java.time.Instant;
import java.util.List;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.Size;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public
class Message {

  @NotBlank
  private String topic;
  private Instant timestamp;
  @Positive
  private Integer partition;
  @Size(min = 1)
  private String key;
  private byte[] value;
  private List<Header> headers;

  @Data
  @Accessors(chain = true)
  public static class Header {

    @NotBlank
    public String key;
    @NotNull
    public byte[] value;
  }
}
