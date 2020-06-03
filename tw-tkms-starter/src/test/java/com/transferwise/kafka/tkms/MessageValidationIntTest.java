package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.Message;
import javax.validation.ConstraintViolationException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@BaseTestEnvironment
public class MessageValidationIntTest {

  @Autowired
  private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;

  @Test
  public void invalidMessagesDoNotPassValidation() {
    assertThatThrownBy(() -> {
      transactionalKafkaMessageSender.sendMessage(new Message());
    }).isInstanceOf(ConstraintViolationException.class).hasMessage("topic: must not be blank");
  }
}
