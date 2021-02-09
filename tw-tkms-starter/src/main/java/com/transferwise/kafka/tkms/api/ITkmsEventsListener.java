package com.transferwise.kafka.tkms.api;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface ITkmsEventsListener {

  /**
   * Kafka has acknowledged the message.
   */
  default void messageAcknowledged(MessageAcknowledgedEvent event) {
  }

  @Data
  @Accessors(chain = true)
  class MessageAcknowledgedEvent {

    /**
     * Id in database table.
     */
    private Long storageId;

    /**
     * Shard-partition message was put into.
     *
     * <p>You can determine the table's name by that.
     */
    private TkmsShardPartition shardPartition;

    /**
     * Kafa client's ProducerRecord object.
     */
    private ProducerRecord<String, byte[]> producerRecord;
  }

  /**
   * It is important to notice, that this is called in an ongoing transaction and there are no guarantees the message will be actually committed.
   */
  default void messageRegistered(MessageRegisteredEvent event) {
  }

  @Data
  @Accessors(chain = true)
  class MessageRegisteredEvent {

    /**
     * Id in database table.
     */
    private Long storageId;

    /**
     * Shard-partition message was put into.
     *
     * <p>You can determine the table's name by that.
     */
    private TkmsShardPartition shardPartition;

    /**
     * The message itself.
     */
    private TkmsMessage message;
  }

}
