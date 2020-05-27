package com.transferwise.kafka.tkms.api;

import javax.validation.Valid;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ITransactionalKafkaMessageSender {

  long sendMessage(@Valid Message message);

  @Data
  @Accessors(chain = true)
  class SendMessageResult {

    private Long id;
    private int shard;
    private int partition;
  }
}
