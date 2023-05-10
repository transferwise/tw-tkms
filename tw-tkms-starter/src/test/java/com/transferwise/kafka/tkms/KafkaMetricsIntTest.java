package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.TestMessagesListener;
import com.transferwise.kafka.tkms.test.TestProperties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class KafkaMetricsIntTest extends BaseIntTest {

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;

  @Autowired
  private TestMessagesListener testMessagesListener;

  @Autowired
  private TestProperties testProperties;
  
  @Autowired
  protected ITransactionsHelper transactionsHelper;

  @AfterEach
  public void cleanup() {
    tkmsRegisteredMessagesCollector.clear();
    tkmsSentMessagesCollector.clear();

    TkmsClockHolder.reset();
  }

  @Test
  void testThatProducerMetricShowsSentMessage() {
    String message = "Hello Toomas!";

    AtomicInteger receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> ExceptionUtils.doUnchecked(() -> {
      TestMessagesListener.TestEvent receivedEvent = objectMapper.readValue(cr.value(), TestMessagesListener.TestEvent.class);
      if (receivedEvent.getMessage().equals(message)) {
        receivedCount.incrementAndGet();
      } else {
        throw new IllegalStateException("Wrong message receive: " + receivedEvent.getMessage());
      }
    });

    testMessagesListener.registerConsumer(messageCounter);
    try {
      TestMessagesListener.TestEvent testEvent = new TestMessagesListener.TestEvent().setId(1L).setMessage(message);

      transactionsHelper.withTransaction().run(() ->
          transactionalKafkaMessageSender
              .sendMessage(new TkmsMessage().setTopic(testProperties.getTestTopic())
                  .setValue(ExceptionUtils.doUnchecked(() -> objectMapper.writeValueAsBytes(testEvent)))));

      await().until(() -> receivedCount.get() > 0);
      await().until(() -> getTablesRowsCount() == 0);
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }

    assertThat(meterRegistry.find("kafka.producer.record.send.total").tags().functionCounter().count())
        .as("Producer's metric shows one message sent.").isPositive();
  }
}
