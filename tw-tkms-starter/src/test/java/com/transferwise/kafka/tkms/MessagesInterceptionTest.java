package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptor.MessageInterceptionDecision;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender.SendMessagesRequest;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.TestMessagesInterceptor;
import com.transferwise.kafka.tkms.test.TestProperties;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
class MessagesInterceptionTest extends BaseIntTest {

  @Autowired
  private TestMessagesInterceptor testMessagesInterceptor;
  @Autowired
  private TransactionalKafkaMessageSender transactionalKafkaMessageSender;
  @Autowired
  private TestProperties testProperties;
  @Autowired
  private ITransactionsHelper transactionsHelper;

  @AfterEach
  void cleanupTest() {
    testMessagesInterceptor.setBeforeSendingToKafkaFunction(null);
  }

  @Test
  void messagesCanBeIntercepted() {
    byte[] someValue = "Hello Kaspar!".getBytes(StandardCharsets.UTF_8);

    final AtomicInteger messageInterceptionsCount = new AtomicInteger();

    testMessagesInterceptor.setBeforeSendingToKafkaFunction(records -> {
      Map<Integer, MessageInterceptionDecision> result = new HashMap<>();
      records.forEach((k, v) -> {
        String key = v.key();
        log.info("Intercepting record '" + key + "'.");
        if ("A".equals(key)) {
          result.put(k, MessageInterceptionDecision.NEUTRAL);
        } else if ("B".equals(key)) {
          if (messageInterceptionsCount.getAndIncrement() < 2) {
            log.info("Retrying '" + key + "'.");
            result.put(k, MessageInterceptionDecision.RETRY);
          } else {
            log.info("Allowing '" + key + "' to continue.");
            result.put(k, MessageInterceptionDecision.NEUTRAL);
          }
        } else {
          result.put(k, MessageInterceptionDecision.DISCARD);
        }
      });
      return result;
    });

    String topic = testProperties.getTestTopic();

    transactionsHelper.withTransaction().run(() ->
        transactionalKafkaMessageSender.sendMessages(
            new SendMessagesRequest()
                .addTkmsMessage(
                    new TkmsMessage()
                        .setUuid(UuidUtils.generatePrefixCombUuid())
                        .setTopic(topic)
                        .setKey("A")
                        .setValue(someValue)
                )
                .addTkmsMessage(
                    new TkmsMessage()
                        .setUuid(UuidUtils.generatePrefixCombUuid())
                        .setTopic(topic)
                        .setKey("B")
                        .setValue(someValue)
                )
                .addTkmsMessage(
                    new TkmsMessage()
                        .setUuid(UuidUtils.generatePrefixCombUuid())
                        .setTopic(topic)
                        .setKey("C")
                        .setValue(someValue)
                )
        )
    );

    await().until(() -> tkmsSentMessagesCollector.getSentMessages(topic).size() == 2);

    assertThat(messageInterceptionsCount.get()).isEqualTo(3);
    var messages = tkmsSentMessagesCollector.getSentMessages(topic);
    assertThat(messages.size()).isEqualTo(2);
    assertThat(messages.get(0).getProducerRecord().key()).isEqualTo("A");
    assertThat(messages.get(1).getProducerRecord().key()).isEqualTo("B");
  }
}
