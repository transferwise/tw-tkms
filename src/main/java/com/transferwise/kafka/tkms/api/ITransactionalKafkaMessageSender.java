package com.transferwise.kafka.tkms.api;

import com.transferwise.kafka.tkms.Message;
import javax.validation.Valid;

public interface ITransactionalKafkaMessageSender {

  long sendMessage(@Valid Message message);

}
