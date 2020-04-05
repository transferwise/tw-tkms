package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.Message;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import io.micrometer.core.instrument.MeterRegistry;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

public class TransactionalKafkaMessageSender implements ITransactionalKafkaMessageSender {

  @Autowired
  private ITkmsDao transactionalKafkaMessageSenderDao;
  @Autowired
  private Validator validator;
  @Autowired
  private MeterRegistry meterRegistry;

  @Override
  @Transactional
  public long sendMessage(@Valid Message message) {
    validateMessage(message);

    long id = -1;
    try {
      id = transactionalKafkaMessageSenderDao.insertMessage(message);
    } finally {
      meterRegistry.counter("tw.tkms.sent", "topic", message.getTopic(), "success", id == -1 ? "false" : "true").increment();
    }
    return id;
  }

  /**
   * Can not trust the @Valid annotation.
   */
  private void validateMessage(Message message) {
    Errors errors = new BeanPropertyBindingResult(message, "message");
    validator.validate(message, errors);
    if (errors.hasErrors()) {
      throw new IllegalArgumentException(errors.toString());
    }
  }

}
