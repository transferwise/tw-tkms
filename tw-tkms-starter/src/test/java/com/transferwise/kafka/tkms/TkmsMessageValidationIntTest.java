package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.BaseTestEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;

@BaseTestEnvironment
class TkmsMessageValidationIntTest extends BaseIntTest {

  @Autowired
  private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;
  @Autowired
  private ITransactionsHelper transactionsHelper;

  @AfterEach
  public void cleanup() {
    tkmsProperties.setDeferMessageRegistrationUntilCommit(false);
  }

  protected void setupConfig(boolean deferUntilCommit) {
    tkmsProperties.setDeferMessageRegistrationUntilCommit(deferUntilCommit);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void invalidMessagesDoNotPassValidation(boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);

    assertThatThrownBy(
        () -> transactionsHelper.withTransaction().run(() ->
            transactionalKafkaMessageSender.sendMessage(new TkmsMessage())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("0: No topic provided.");
  }
}
