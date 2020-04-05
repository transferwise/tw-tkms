package com.transferwise.kafka.tkms.api;

import javax.validation.Valid;

public interface ITransactionalKafkaMessageSender {

  @SuppressWarnings("UnusedReturnValue")
  long sendMessage(@Valid Message message);

}
