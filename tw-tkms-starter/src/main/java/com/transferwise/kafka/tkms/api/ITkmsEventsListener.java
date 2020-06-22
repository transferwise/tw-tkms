package com.transferwise.kafka.tkms.api;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface ITkmsEventsListener {

  default void messageAcknowledged(MessageAcknowledgedEvent event) {
  }

  @Data
  @Accessors(chain = true)
  class MessageAcknowledgedEvent {

    private Long id;
    private ProducerRecord<String, byte[]> producerRecord;
  }

  /**
   * It is important to notice, that in a transaction, message registration may not be commited yet, at this point.
   */
  default void messageRegistered(MessageRegisteredEvent event) {
  }

  @Data
  @Accessors(chain = true)
  class MessageRegisteredEvent {

    private Long storageId;

    private ShardPartition shardPartition;

    private TkmsMessage message;
  }

}
