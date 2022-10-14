package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.test.BaseTestEnvironment;
import com.transferwise.kafka.tkms.test.ITkmsRegisteredMessagesCollector;
import com.transferwise.kafka.tkms.test.ITkmsSentMessagesCollector;
import com.transferwise.kafka.tkms.test.TestMessagesListener;
import com.transferwise.kafka.tkms.test.TestProperties;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@BaseTestEnvironment
public class KafkaMetricsIntTest {
  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;

  @Autowired
  private TestMessagesListener testMessagesListener;

  @Autowired
  private TestProperties testProperties;

  @Autowired
  protected ITkmsRegisteredMessagesCollector tkmsRegisteredMessagesCollector;

  @Autowired
  protected ITkmsSentMessagesCollector tkmsSentMessagesCollector;

  @Autowired
  protected MeterRegistry meterRegistry;

  @AfterEach
  public void cleanup() {
    tkmsRegisteredMessagesCollector.clear();
    tkmsSentMessagesCollector.clear();

    TkmsClockHolder.reset();
  }

  @Test
  void testThatProducerMetricShowsSentMessage() throws Exception {

    String message = "Hello World!";

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

      transactionalKafkaMessageSender
        .sendMessage(new TkmsMessage().setTopic(testProperties.getTestTopic()).setValue(objectMapper.writeValueAsBytes(testEvent)));

      await().until(() -> receivedCount.get() > 0);

    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }

    assertThat(meterRegistry.find("kafka.producer.record.send.total").tags().functionCounter().count())
      .as("Producer's metric shows one message sent.").isGreaterThan(0L);
  }
}
