package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.test.BaseTestEnvironment;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@BaseTestEnvironment
class TkmsMessageValidationIntTest {

  @Autowired
  private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;
  @Autowired
  private ITransactionsHelper transactionsHelper;

  @Test
  void invalidMessagesDoNotPassValidation() {
    assertThatThrownBy(
        () -> transactionsHelper.withTransaction().run(() ->
            transactionalKafkaMessageSender.sendMessage(new TkmsMessage())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("0: No topic provided.");
  }
}
