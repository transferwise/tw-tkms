package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.Message;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.dao.ITkmsDao.InsertMessageResult;
import com.transferwise.kafka.tkms.metrics.IMetricsTemplate;
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
  private IMetricsTemplate metricsTemplate;

  @Override
  @Transactional
  public long sendMessage(@Valid Message message) {
    //TODO: Check if topic exists here, when server does not support auto creation of those?
    validateMessage(message);

    InsertMessageResult insertMessageResult = null;
    try {
      insertMessageResult = transactionalKafkaMessageSenderDao.insertMessage(message);
      return insertMessageResult.getId();
    } finally {
      if (insertMessageResult == null) {
        metricsTemplate.recordMessageRegistering(message.getTopic(), null, false);
      } else {
        metricsTemplate.recordMessageRegistering(message.getTopic(), insertMessageResult.getShardPartition(), true);
      }
    }
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
