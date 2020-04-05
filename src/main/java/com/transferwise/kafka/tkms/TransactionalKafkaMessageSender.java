package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
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

  @Override
  @Transactional
  public long sendMessage(@Valid Message message) {
    validateMessage(message);

    return transactionalKafkaMessageSenderDao.insertMessage(message, 0);
  }

  /**
   * Can not trust the @Valid annotation.
   */
  private void validateMessage(Message message) {
    Errors errors = new BeanPropertyBindingResult(message, "message");
    validator.validate(message, errors);
    if (errors.hasErrors()) {
      //TODO: PII?
      throw new IllegalArgumentException(errors.toString());
    }
  }

}
