package com.transferwise.kafka.tkms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender.SendMessagesRequest;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender.SendMessagesResult;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsMessage.Header;
import com.transferwise.kafka.tkms.metrics.TkmsMetricsTemplate;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.BaseTestEnvironment;
import com.transferwise.kafka.tkms.test.TestMessagesListener;
import com.transferwise.kafka.tkms.test.TestMessagesListener.TestEvent;
import com.transferwise.kafka.tkms.test.TestProperties;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@BaseTestEnvironment
@Slf4j
public class EndToEndIntTest extends BaseIntTest {

  @Autowired
  private ObjectMapper objectMapper;
  @Autowired
  private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;
  @Autowired
  private TestMessagesListener testMessagesListener;
  @Autowired
  private ITransactionsHelper transactionsHelper;
  @Autowired
  private TestProperties testProperties;

  @Test
  public void testThatJsonStringMessageCanBeSentAndRetrieved() throws Exception {
    var messagePart = "Hello World!";
    int messageMultiplier = 100;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < messageMultiplier; i++) {
      sb.append(messagePart);
    }
    String message = sb.toString();

    AtomicInteger receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> ExceptionUtils.doUnchecked(() -> {
      TestEvent receivedEvent = objectMapper.readValue(cr.value(), TestEvent.class);
      if (receivedEvent.getMessage().equals(message)) {
        assertThat(cr.headers().toArray().length).isEqualTo(1);
        org.apache.kafka.common.header.Header header = cr.headers().toArray()[0];
        assertThat(header.key()).isEqualTo("x-tw-criticality");
        assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo("PrettyLowLol");
        receivedCount.incrementAndGet();
      } else {
        throw new IllegalStateException("Wrong message receive: " + receivedEvent.getMessage());
      }
    });

    testMessagesListener.registerConsumer(messageCounter);
    try {
      TestEvent testEvent = new TestEvent().setId(1L).setMessage(message);

      transactionalKafkaMessageSender
          .sendMessage(new TkmsMessage().setTopic(testProperties.getTestTopic()).setValue(objectMapper.writeValueAsBytes(testEvent))
              .addHeader(new Header().setKey("x-tw-criticality").setValue("PrettyLowLol".getBytes(StandardCharsets.UTF_8))));

      await().until(() -> receivedCount.get() > 0);

      log.info("Messages received: " + receivedCount.get());
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }

    assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(testProperties.getTestTopic()).size()).isEqualTo(1);
  }

  @Test
  public void testExactlyOnceDelivery() throws Exception {
    String message = "Hello World!";
    int threadsCount = 20;
    int batchesCount = 20;
    int batchSize = 20;
    int messagesCount = threadsCount * batchesCount * batchSize;

    ConcurrentHashMap<Long, AtomicInteger> receivedMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, AtomicInteger> partitionsMap = new ConcurrentHashMap<>();

    var receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> ExceptionUtils.doUnchecked(() -> {
      TestEvent receivedEvent = objectMapper.readValue(cr.value(), TestEvent.class);
      if (receivedEvent.getMessage().equals(message)) {
        receivedMap.computeIfAbsent(receivedEvent.getId(), (k) -> new AtomicInteger()).incrementAndGet();
        partitionsMap.computeIfAbsent(cr.partition(), (k) -> new AtomicInteger()).incrementAndGet();
        receivedCount.incrementAndGet();
      } else {
        throw new IllegalStateException("Wrong message receive: " + receivedEvent.getMessage());
      }
    });

    testMessagesListener.registerConsumer(messageCounter);
    try {
      Thread[] threads = new Thread[threadsCount];
      for (long t = 0; t < threadsCount; t++) {
        long finalT = t;
        threads[(int) t] = new Thread(() -> {
          for (long b = 0; b < batchesCount; b++) {
            long finalB = b;
            transactionsHelper.withTransaction().call(() -> {
              for (long i = 0; i < batchSize; i++) {
                long id = finalT * threadsCount * batchesCount + finalB * batchesCount + i;
                TestEvent testEvent = new TestEvent().setId(id).setMessage(message);
                transactionalKafkaMessageSender
                    .sendMessage(new TkmsMessage().setTopic(testProperties.getTestTopic()).setValue(objectMapper.writeValueAsBytes(testEvent)));
              }
              return null;
            });
          }
        });
      }
      final long startTimeMs = System.currentTimeMillis();
      for (Thread thread : threads) {
        thread.start();
      }
      for (Thread thread : threads) {
        thread.join();
      }

      await().until(() -> receivedCount.get() >= messagesCount);

      log.info("Messages received: " + receivedCount.get());

      for (long i = 0; i < messagesCount; i++) {
        AtomicInteger ai = receivedMap.get(i);
        int cnt = ai == null ? 0 : ai.get();
        if (cnt != 1) {
          throw new IllegalStateException("Unexpected count of messages for id " + i + ": " + cnt);
        }
      }

      // Starting from kafka-clients 2.5, another type of efficient partitioning is used, which does not guarantee
      // that all partitions receive messages with that kind of dataset.
      assertThat(partitionsMap.entrySet().size()).as("At least some partitions received messages").isGreaterThan(2);
      partitionsMap.forEach((key, value) -> log.info("Partition " + key + " received " + value.get() + " messages."));

      log.info("Sending " + messagesCount + " messages took " + (System.currentTimeMillis() - startTimeMs + " ms."));

    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @Test
  public void testThatMessagesWithSameKeyEndUpInOnePartition() throws Exception {
    String message = "Hello World!";
    String key = "GrailsRocks";
    int n = 20;
    ConcurrentHashMap<Integer, AtomicInteger> partitionsMap = new ConcurrentHashMap<>();
    AtomicInteger receivedCount = new AtomicInteger();

    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> {
      partitionsMap.computeIfAbsent(cr.partition(), (k) -> new AtomicInteger()).incrementAndGet();
      receivedCount.incrementAndGet();
    };

    testMessagesListener.registerConsumer(messageCounter);
    try {
      for (int i = 0; i < n; i++) {
        TestEvent testEvent = new TestEvent().setId(1L).setMessage(message);

        transactionalKafkaMessageSender
            .sendMessage(new TkmsMessage().setKey(key).setTopic(testProperties.getTestTopic()).setValue(objectMapper.writeValueAsBytes(testEvent)));
      }
      await().until(() -> receivedCount.get() >= n);

      log.info("Messages received: " + receivedCount.get());

      assertThat(partitionsMap.entrySet().size()).isEqualTo(1);
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @Test
  public void testThatMessagesWithSamePartitionEndUpInOnePartition() throws Exception {
    String message = "Hello World!";
    int partition = 3;
    int n = 20;
    ConcurrentHashMap<Integer, AtomicInteger> partitionsMap = new ConcurrentHashMap<>();
    AtomicInteger receivedCount = new AtomicInteger();

    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> {
      partitionsMap.computeIfAbsent(cr.partition(), (k) -> new AtomicInteger()).incrementAndGet();
      receivedCount.incrementAndGet();
    };

    testMessagesListener.registerConsumer(messageCounter);
    try {
      for (int i = 0; i < n; i++) {
        TestEvent testEvent = new TestEvent().setId(1L).setMessage(message);

        transactionalKafkaMessageSender
            .sendMessage(
                new TkmsMessage().setPartition(partition).setTopic(testProperties.getTestTopic())
                    .setValue(objectMapper.writeValueAsBytes(testEvent)));
      }
      await().until(() -> receivedCount.get() >= n);

      log.info("Messages received: " + receivedCount.get());

      assertThat(partitionsMap.entrySet().size()).isEqualTo(1);
      assertThat(partitionsMap.get(partition).get()).isEqualTo(n);
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @Test
  public void testThatMessagesOrderForAnEntityIsPreserved() throws Exception {
    String message = "Hello World!";
    int entitiesCount = 100;
    int entityEventsCount = 100;
    int messagesCount = entitiesCount * entityEventsCount;

    ConcurrentHashMap<Long, List<Long>> receivedMap = new ConcurrentHashMap<>();
    AtomicInteger receivedCount = new AtomicInteger();

    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> ExceptionUtils.doUnchecked(() -> {
      TestEvent receivedEvent = objectMapper.readValue(cr.value(), TestEvent.class);
      receivedMap.computeIfAbsent(receivedEvent.getEntityId(), (k) -> new CopyOnWriteArrayList<>()).add(receivedEvent.getId());
      receivedCount.incrementAndGet();
    });

    testMessagesListener.registerConsumer(messageCounter);
    try {
      Thread[] threads = new Thread[entitiesCount];
      for (long e = 0; e < entitiesCount; e++) {
        long finalE = e;
        threads[(int) e] = new Thread(() -> {
          for (long i = 0; i < entityEventsCount; i++) {
            long id = finalE * entityEventsCount + i;
            TestEvent testEvent = new TestEvent().setId(id).setEntityId(finalE).setMessage(message);
            ExceptionUtils.doUnchecked(() -> {
              transactionalKafkaMessageSender
                  .sendMessage(new TkmsMessage().setKey(String.valueOf(finalE)).setTopic(testProperties.getTestTopic())
                      .setValue(objectMapper.writeValueAsBytes(testEvent)));
            });
          }
        });
      }
      for (Thread thread : threads) {
        thread.start();
      }
      final long startTimeMs = System.currentTimeMillis();
      for (Thread thread : threads) {
        thread.join();
      }

      await().atMost(Duration.ofSeconds(10)).until(() -> receivedCount.get() >= messagesCount);

      log.info("Messages received: " + receivedCount.get());
      log.info("Messages sent in " + (System.currentTimeMillis() - startTimeMs) + " ms.");
      
      for (long i = 0; i < entitiesCount; i++) {
        List<Long> entityEventsIds = receivedMap.get(i);
        assertThat(entityEventsIds.size()).isEqualTo(entityEventsCount);
        for (int j = 0; j < entityEventsIds.size(); j++) {
          if (j > 0 && entityEventsIds.get(j) < entityEventsIds.get(j - 1)) {
            throw new IllegalStateException("Invalid order detected for entity " + i);
          }
        }
      }
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @Test
  @SneakyThrows
  public void sendingToUnknownTopicWillBePreventedWhenTopicAutoCreationIsDisabled() {
    assertThatThrownBy(() -> transactionalKafkaMessageSender
        .sendMessage(new TkmsMessage().setTopic("NotExistingTopic").setValue("Stuff".getBytes(StandardCharsets.UTF_8))))
        .hasMessageContaining("Topic NotExistingTopic not present in metadata");
  }

  @Test
  public void sendingMultipleMessagesWorks() {
    byte[] value = "{\"message\" : \"Hello World!\"}".getBytes(StandardCharsets.UTF_8);

    AtomicInteger receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter =
        cr -> ExceptionUtils.doUnchecked(receivedCount::incrementAndGet);

    testMessagesListener.registerConsumer(messageCounter);

    String topic = testProperties.getTestTopic();
    SendMessagesResult sendMessagesResult = transactionalKafkaMessageSender.sendMessages(new SendMessagesRequest()
        .addTkmsMessage(new TkmsMessage().setTopic(topic).setValue(value))
        .addTkmsMessage(new TkmsMessage().setTopic(topic).setValue(value).setShard(1))
        .addTkmsMessage(new TkmsMessage().setTopic(topic).setValue(value).setShard(0).setPartition(0))
        .addTkmsMessage(new TkmsMessage().setTopic(topic).setValue(value).setShard(0).setPartition(1))
    );

    assertThat(sendMessagesResult.getResults().size()).isEqualTo(4);
    assertThat(sendMessagesResult.getResults().get(1).getStorageId()).isNotNull();
    assertThat(sendMessagesResult.getResults().get(1).getShardPartition().getShard()).isEqualTo(1);
    assertThat(sendMessagesResult.getResults().get(2).getShardPartition().getShard()).isEqualTo(0);

    try {
      await().until(() -> receivedCount.get() == 4);

      log.info("Messages received: " + receivedCount.get());
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }

    assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(topic).size()).isEqualTo(4);

    assertThat(meterRegistry.find(TkmsMetricsTemplate.INTERFACE_MESSAGE_REGISTERED).tag("shard", "0").counter().count()).isEqualTo(3);
    assertThat(meterRegistry.find(TkmsMetricsTemplate.INTERFACE_MESSAGE_REGISTERED).tag("shard", "1").counter().count()).isEqualTo(1);
  }

  @Test
  public void testThatSendingLargeMessagesWillNotCauseAnIssue() {
    StringBuilder sb = new StringBuilder();
    // We generate message as large as maximum allowed bytes but this is set up to fail, as there is additional information
    // added by kafka producer.
    for (int i = 0; i < 10485760; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(1));
    }
    final MutableObject<String> message = new MutableObject<>(sb.toString());

    AtomicInteger receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> ExceptionUtils.doUnchecked(() -> {
      String receivedValue = cr.value();
      if (receivedValue.equals(message.getValue())) {
        receivedCount.incrementAndGet();
      } else {
        throw new IllegalStateException("Wrong message received.");
      }
    });

    testMessagesListener.registerConsumer(messageCounter);
    try {
      assertThatThrownBy(() ->
          transactionalKafkaMessageSender
              .sendMessage(
                  new TkmsMessage().setTopic(testProperties.getTestTopic()).setValue(message.getValue().getBytes(StandardCharsets.US_ASCII))))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("0: Estimated message size is 10485878, which is larger than maximum of 10485760.");

      assertThatThrownBy(() ->
          transactionalKafkaMessageSender
              .sendMessages(new SendMessagesRequest().addTkmsMessage(
                  new TkmsMessage().setTopic(testProperties.getTestTopic()).setValue(message.getValue().getBytes(StandardCharsets.US_ASCII)))))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("0: Estimated message size is 10485878, which is larger than maximum of 10485760.");

      message.setValue(message.getValue().substring(0, 10484000));
      transactionalKafkaMessageSender
          .sendMessage(new TkmsMessage().setTopic(testProperties.getTestTopic()).setValue(message.getValue().getBytes(StandardCharsets.US_ASCII)));

      await().until(() -> receivedCount.get() > 0);

      log.info("Messages received: " + receivedCount.get());
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }

    assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(testProperties.getTestTopic()).size()).isEqualTo(1);
  }
}
