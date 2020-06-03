package com.transferwise.kafka.tkms.api;

import lombok.Data;
import lombok.experimental.Accessors;

public interface ITransactionalKafkaMessageSender {

  long sendMessage(Message message);

  @Data
  @Accessors(chain = true)
  class SendMessageResult {

    private Long id;
    private int shard;
    private int partition;
  }
}
