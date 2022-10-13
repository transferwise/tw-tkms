package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.TestMessagesListener;
import com.transferwise.kafka.tkms.test.TestProperties;
import io.micrometer.core.instrument.Gauge;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
class MonitoringIntTest extends BaseIntTest {

  @Autowired
  private ObjectMapper objectMapper;
  @Autowired
  private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;
  @Autowired
  private TestMessagesListener testMessagesListener;
  @Autowired
  private TestProperties testProperties;

  @Test
  void testThatMonitoringMetricsArePresent() {
    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find("tw.tkms.dao.approximate.messages.count").tags("shard", "0", "partition", "0").gauge();
      return gauge != null && gauge.value() >= 0;
    });

    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find("tw.tkms.dao.approximate.messages.count").tags("shard", "1", "partition", "0").gauge();
      return gauge != null && gauge.value() >= 0;
    });
  }

  @Test
  void testThatTableStatsMetricsArePresent() {
    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find("tw.tkms.dao.rows.in.table.stats").tags("shard", "1", "partition", "0").gauge();
      return gauge != null && gauge.value() == 1_000_000;
    });

    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find("tw.tkms.dao.rows.in.index.stats").tags("shard", "1", "partition", "0").gauge();
      return gauge != null && gauge.value() == 1_000_000;
    });
  }

  @Test
  void earliestMessageIdIsRegistered() {
    Awaitility.await().until(() -> {
      Gauge gauge = meterRegistry.find("tw.tkms.dao.earliest.message.id").tags("shard", "1", "partition", "0").gauge();
      return gauge != null;
    });

    assertThat(meterRegistry.find("tw.tkms.dao.earliest.message.id").tags("shard", "0", "partition", "0").gauge())
        .as("Earliest message id tracking is not enabled for shard 0.").isNull();
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

    assertThat(meterRegistry.find("kafka.producer.record.send.total")
                .tags("client.id", "producer-1", "kafka.version", "2.7.2")
                .functionCounter().count())
            .as("Producer's metric shows one message sent.")
            .isEqualTo(1L);
  }

}
