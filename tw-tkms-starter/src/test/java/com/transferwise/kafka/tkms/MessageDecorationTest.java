package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptor.MessageInterceptionDecision;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender.SendMessagesRequest;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.ITkmsSentMessagesCollector.SentMessage;
import com.transferwise.kafka.tkms.test.TestMessagesInterceptor;
import com.transferwise.kafka.tkms.test.TestProperties;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class MessageDecorationTest extends BaseIntTest {

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
  void messagesAreDecorateWithJambi() {
    byte[] someValue = "Here from the king's mountain view, Feast like a sultan I do!".getBytes(StandardCharsets.UTF_8);

    String topic = testProperties.getTestTopic();

    transactionsHelper.withTransaction().run(() ->
        transactionalKafkaMessageSender.sendMessages(new SendMessagesRequest()
            .addTkmsMessage(new TkmsMessage().setTopic(topic).setKey("A").setValue(someValue))
            .addTkmsMessage(new TkmsMessage().setTopic(topic).setKey("B").setValue(someValue))
        ));

    await().until(() -> tkmsSentMessagesCollector.getSentMessages(topic).size() == 2);

    var messages = tkmsSentMessagesCollector.getSentMessages(topic);
    assertThat(messages.size()).isEqualTo(2);
    checkForHeader(messages.get(0) , "adam-jones" , "jambi");
    checkForHeader(messages.get(1) , "danny-carey" , "the-grudge");
  }

  private void checkForHeader(SentMessage sentMessage, String key, String value) {
    assertTrue(
        StreamSupport.stream(sentMessage.getProducerRecord().headers().spliterator() , false)
        .anyMatch(h -> h.key().equals(key) && value.equals(new String(h.value())))
    );
  }
}
