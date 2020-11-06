package com.transferwise.kafka.tkms.api;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;
import javax.validation.constraints.Size;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TkmsMessage {
  
  /**
   * Kafka topic.
   */
  @NotBlank
  private String topic;
  /**
   * Kafka message timestamp.
   *
   * <p>The one you would put into ProducerRecord.
   */
  private Instant timestamp;
  /**
   * Kafka topic's partition.
   *
   * <p>Has higher priority than the key.
   */
  @PositiveOrZero
  private Integer partition;
  /**
   * Kafka message's key.
   *
   * <p>Will be used to determine the partition.
   */
  @Size(min = 1)
  private String key;
  /**
   * Kafka message's content/payload.
   */
  @SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
  @NotNull
  private byte[] value;
  
  /**
   * Tkms shard.
   */
  @PositiveOrZero
  private Integer shard;

  /**
   * Kafka message's headers.
   */
  private List<Header> headers;

  public TkmsMessage addHeader(Header header) {
    if (headers == null) {
      headers = new ArrayList<>();
    }
    headers.add(header);
    return this;
  }


  @Data
  @Accessors(chain = true)
  public static class Header {

    /**
     * Kafka message header's key.
     */
    @NotBlank
    public String key;
    /**
     * Kafka message header's value.
     */
    @NotNull
    public byte[] value;
  }
}
