package com.transferwise.kafka.tkms;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender.SendMessagesRequest;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.ITkmsSentMessagesCollector.SentMessage;
import com.transferwise.kafka.tkms.test.TestMessagesInterceptor;
import com.transferwise.kafka.tkms.test.TestProperties;
import java.nio.charset.StandardCharsets;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

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
            .addTkmsMessage(new TkmsMessage().setTopic(topic).setKey("adam-jones").setShard(4).setValue(someValue))
            .addTkmsMessage(new TkmsMessage().setTopic(topic).setKey("danny-carey").setPartition(5).setValue(someValue))
        ));

    await().until(() -> tkmsSentMessagesCollector.getSentMessages(topic).size() == 2);
    var messages = tkmsSentMessagesCollector.getSentMessages(topic);

    assertEquals(2, messages.size());
    checkForHeader(messages.get(0), "tool", "jambi");
    assertEquals(0, messages.get(0).getShardPartition().getShard());
    assertEquals(0, messages.get(0).getShardPartition().getPartition());
    checkForHeader(messages.get(1), "tool", "jambi");
    assertEquals(0, messages.get(1).getShardPartition().getShard());
    assertEquals(0, messages.get(1).getShardPartition().getPartition());
  }

  private void checkForHeader(SentMessage sentMessage, String key, String value) {
    assertTrue(
        StreamSupport.stream(sentMessage.getProducerRecord().headers().spliterator(), false)
            .anyMatch(h -> h.key().equals(key) && value.equals(new String(h.value(), StandardCharsets.UTF_8)))
    );
  }
}
