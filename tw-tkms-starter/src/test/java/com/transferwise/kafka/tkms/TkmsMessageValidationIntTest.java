package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.BaseTestEnvironment;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;

@BaseTestEnvironment
class TkmsMessageValidationIntTest extends BaseIntTest {

  @Autowired
  private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;
  @Autowired
  private ITransactionsHelper transactionsHelper;

  private boolean uuidHeaderRequired;

  @BeforeEach
  public void saveConfig() {
    uuidHeaderRequired = tkmsProperties.isUuidHeaderRequired();
  }

  @AfterEach
  public void cleanup() {
    tkmsProperties.setDeferMessageRegistrationUntilCommit(false);
    tkmsProperties.setUuidHeaderRequired(uuidHeaderRequired);
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

  @Test
  void failsToSendMessageThatHasNoUuidWhenUuidHeaderIsRequired() {
    tkmsProperties.setUuidHeaderRequired(true);

    assertThatThrownBy(
        () -> transactionsHelper.withTransaction().run(
            () -> transactionalKafkaMessageSender.sendMessage(
                new TkmsMessage()
                    .setTopic(testProperties.getTestTopic())
                    .setValue(new byte[1])
            )
        )
    ).isInstanceOf(IllegalArgumentException.class)
        .hasMessage("0: Message is required to have @{x-wise-uuid} header.");
  }

  @Test
  void doesNotRequireUuidPresentIfItIsNotRequiredByConfiguration() {
    tkmsProperties.setUuidHeaderRequired(false);

    transactionsHelper.withTransaction().run(
        () -> transactionalKafkaMessageSender.sendMessage(
            new TkmsMessage()
                .setTopic(testProperties.getTestTopic())
                .setValue(new byte[1])
        )
    );

    Awaitility.await().until(() -> tkmsSentMessagesCollector.getSentMessages(testProperties.getTestTopic()).size() == 1);
  }
}
