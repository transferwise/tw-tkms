package com.transferwise.kafka.tkms.api;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.kafka.tkms.CompressionAlgorithm;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

  /**
   * Allows to add any metadata to the message object for correlation in event handlers.
   *
   * <p>The metadata will not be used in any way for creating and sending Kafka messages.
   *
   * <p>For example, you have `deferMessageRegistrationUntilCommit` enabled, but need to know/log Tkms messages' storage ids.
   * Then you could add some kind of key into the metadata, register an `ITkmsEventsListener` and use that metadata to distinguish specific messages
   * from others.
   */
  private Map<?, ?> metadata;

  /**
   * Adds {@code x-wise-uuid} header to the message, which uniquely identifies this message for consumers.
   *
   * <p>Having UUID in header allows consumers to run deduplication check on this value without need to deserialize payload.
   * If payload provides uuid it must be the same as this value so that consumers that depend on either of these values can have consistent
   * deduplication.
   *
   * <p>Prefer using sequential uuids (e.g. {@link UuidUtils#generatePrefixCombUuid()}) which are proved to yield better performance.
   */
  public TkmsMessage addUuidHeader(UUID uuid) {
    return addHeader(
        new Header()
            .setKey("x-wise-uuid")
            .setValue(uuid.toString().getBytes(StandardCharsets.UTF_8))
    );
  }

  /**
   * Adds {@code x-wise-priority} header to the message, which defines priority of this message for consumers.
   *
   * <p>Lower value means higher priority. For example, 0 is higher priority than 10.
   *
   * <p>Having priority in header allows consumers to derive priority without need to deserialize payload. For example, it can be useful
   * when consumers filter messages based on priority before deciding how to process those.
   */
  public TkmsMessage addPriorityHeader(long priority) {
    return addHeader(
        new Header()
            .setKey("x-wise-priority")
            .setValue(Long.toString(priority).getBytes(StandardCharsets.UTF_8))
    );
  }

  public TkmsMessage addHeader(Header header) {
    if (headers == null) {
      headers = new ArrayList<>();
    }
    headers.add(header);
    return this;
  }

  public TkmsMessage accept(ITkmsMessageDecorator decorator) {
    var headers = decorator.getAdditionalHeaders(this);
    if (headers != null) {
      headers.forEach(this::addHeader);
    }
    var overridedPartition = decorator.getOverridedPartition(this);
    if (overridedPartition != null) {
      setShard(overridedPartition.getShard());
      setPartition(overridedPartition.getPartition());
    }
    return this;
  }

  /**
   * Forces specified compression.
   */
  private Compression compression;

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

  @Data
  @Accessors(chain = true)
  public static class Compression {

    private CompressionAlgorithm algorithm = CompressionAlgorithm.GZIP;

    private Integer blockSize;

    private Integer level;
  }
}
