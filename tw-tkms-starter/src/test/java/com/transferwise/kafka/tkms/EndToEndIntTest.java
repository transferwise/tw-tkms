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
import com.transferwise.kafka.tkms.api.TkmsMessage.Compression;
import com.transferwise.kafka.tkms.api.TkmsMessage.Header;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.dao.FaultInjectedTkmsDao;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.metrics.TkmsMetricsTemplate;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.BaseTestEnvironment;
import com.transferwise.kafka.tkms.test.ITkmsTestDao;
import com.transferwise.kafka.tkms.test.TestMessagesListener;
import com.transferwise.kafka.tkms.test.TestMessagesListener.TestEvent;
import com.transferwise.kafka.tkms.test.TestProperties;
import io.micrometer.core.instrument.Counter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

@BaseTestEnvironment
@Slf4j
class EndToEndIntTest extends BaseIntTest {

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
  @Autowired
  private ITkmsTestDao tkmsTestDao;
  @Autowired
  private TkmsProperties tkmsProperties;
  @Autowired
  private ITkmsDao tkmsDao;
  @Autowired
  private TkmsStorageToKafkaProxy tkmsStorageToKafkaProxy;

  private FaultInjectedTkmsDao faultInjectedTkmsDao;

  @BeforeEach
  public void setup() {
    faultInjectedTkmsDao = new FaultInjectedTkmsDao(tkmsDao);
    tkmsStorageToKafkaProxy.setDao(faultInjectedTkmsDao);
  }

  @AfterEach
  public void cleanup() {
    super.cleanup();
    tkmsStorageToKafkaProxy.setDao(tkmsDao);
  }

  @Test
  void testThatJsonStringMessageCanBeSentAndRetrieved() throws Exception {
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
        assertThat(cr.headers().toArray()).hasSize(1);
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

    assertThatTablesAreEmpty();

    assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(testProperties.getTestTopic()).size()).isEqualTo(1);
  }

  @Test
  void testExactlyOnceDelivery() throws Exception {
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

      assertThatTablesAreEmpty();
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @Test
  void testThatMessagesWithSameKeyEndUpInOnePartition() throws Exception {
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

      assertThatTablesAreEmpty();
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @Test
  void testThatMessagesWithSamePartitionEndUpInOnePartition() throws Exception {
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

      assertThatTablesAreEmpty();
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @Test
  void testThatMessagesOrderForAnEntityIsPreserved() throws Exception {
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
      assertThatTablesAreEmpty();
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @Test
  @SneakyThrows
  void sendingToUnknownTopicWillBePreventedWhenTopicAutoCreationIsDisabled() {
    assertThatThrownBy(() -> transactionalKafkaMessageSender
        .sendMessage(new TkmsMessage().setTopic("NotExistingTopic").setValue("Stuff".getBytes(StandardCharsets.UTF_8))))
        .hasMessageContaining("Topic NotExistingTopic not present in metadata");
  }

  @Test
  void sendingMultipleMessagesWorks() {
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
    assertThat(sendMessagesResult.getResults().get(2).getShardPartition().getShard()).isZero();

    try {
      await().until(() -> receivedCount.get() == 4);

      log.info("Messages received: " + receivedCount.get());
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }

    assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(topic).size()).isEqualTo(4);

    assertThat(meterRegistry.find(TkmsMetricsTemplate.INTERFACE_MESSAGE_REGISTERED).tag("shard", "0").counter().count()).isEqualTo(3);
    assertThat(meterRegistry.find(TkmsMetricsTemplate.INTERFACE_MESSAGE_REGISTERED).tag("shard", "1").counter().count()).isEqualTo(1);

    assertThatTablesAreEmpty();
  }

  @Test
  void testThatSendingLargeMessagesWillNotCauseAnIssue() {
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

    assertThatTablesAreEmpty();
  }

  /**
   * If `TkmsStorageToKafkaProxy has some important lines switched around "lastId" logic, the test will start failing.
   */
  @Test
  void testThatTemporaryDeleteFailureDoesNotLeaveTrashBehind() throws Exception {
    String message = "Hello World!";
    int messagesCount = 1000;

    var receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> ExceptionUtils.doUnchecked(() -> {
      TestEvent receivedEvent = objectMapper.readValue(cr.value(), TestEvent.class);
      if (receivedEvent.getMessage().equals(message)) {
        receivedCount.incrementAndGet();
      } else {
        throw new IllegalStateException("Wrong message receive: " + receivedEvent.getMessage());
      }
    });

    faultInjectedTkmsDao.setDeleteMessagesFails(true);
    testMessagesListener.registerConsumer(messageCounter);
    try {
      for (int i = 0; i < messagesCount; i++) {
        TestEvent testEvent = new TestEvent().setId((long) i).setMessage(message);
        transactionalKafkaMessageSender
            .sendMessage(new TkmsMessage().setTopic(testProperties.getTestTopic()).setValue(objectMapper.writeValueAsBytes(testEvent)));
      }

      await().until(() -> receivedCount.get() >= messagesCount);
      await().until(() -> getTablesRowsCount() == messagesCount);

      faultInjectedTkmsDao.setDeleteMessagesFails(false);

      await().until(() -> getTablesRowsCount() == 0);
      log.info("Messages received: " + receivedCount.get());
    } finally {
      faultInjectedTkmsDao.setDeleteMessagesFails(false);
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  protected void assertThatTablesAreEmpty() {
    for (int s = 0; s < tkmsProperties.getShardsCount(); s++) {
      for (int p = 0; p < tkmsProperties.getPartitionsCount(s); p++) {
        TkmsShardPartition sp = TkmsShardPartition.of(s, p);
        int rowsCount = tkmsTestDao.getMessagesCount(sp);

        assertThat(rowsCount).as("Row count for " + sp + " is zero.").isZero();
      }
    }
  }

  protected int getTablesRowsCount() {
    int count = 0;
    for (int s = 0; s < tkmsProperties.getShardsCount(); s++) {
      for (int p = 0; p < tkmsProperties.getPartitionsCount(s); p++) {
        TkmsShardPartition sp = TkmsShardPartition.of(s, p);
        count += tkmsTestDao.getMessagesCount(sp);
      }
    }
    return count;
  }

  private static Stream<Arguments> compressionInput() {
    return Stream.of(
        Arguments.of(CompressionAlgorithm.GZIP, 104),
        Arguments.of(CompressionAlgorithm.NONE, 1263),
        Arguments.of(CompressionAlgorithm.LZ4, 127),
        Arguments.of(CompressionAlgorithm.SNAPPY, 157),
        Arguments.of(CompressionAlgorithm.SNAPPY_FRAMED, 155),
        Arguments.of(CompressionAlgorithm.ZSTD, 93)
    );
  }

  @ParameterizedTest
  @MethodSource("compressionInput")
  void testMessageIsCompressed(CompressionAlgorithm algorithm, int expectedSerializedSize) throws Exception {
    var message = StringUtils.repeat("Hello World!", 100);

    AtomicInteger receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> ExceptionUtils.doUnchecked(() -> {
      TestEvent receivedEvent = objectMapper.readValue(cr.value(), TestEvent.class);
      if (receivedEvent.getMessage().equals(message)) {
        receivedCount.incrementAndGet();
      } else {
        throw new IllegalStateException("Wrong message received: " + receivedEvent.getMessage());
      }
    });

    Counter counter = meterRegistry.find(TkmsMetricsTemplate.DAO_SERIALIZED_SIZE_BYTES).tag("algorithm", algorithm.name().toLowerCase()).counter();
    double startingSerializedSizeBytes = counter == null ? 0 : counter.count();

    testMessagesListener.registerConsumer(messageCounter);
    try {
      TestEvent testEvent = new TestEvent().setId(1L).setMessage(message);

      transactionalKafkaMessageSender
          .sendMessage(new TkmsMessage().setTopic(testProperties.getTestTopic()).setValue(objectMapper.writeValueAsBytes(testEvent))
              .setCompression(new Compression().setAlgorithm(algorithm)));

      await().until(() -> receivedCount.get() > 0);

      assertThat(meterRegistry.find(TkmsMetricsTemplate.DAO_SERIALIZED_SIZE_BYTES).tag("algorithm", algorithm.name().toLowerCase()).counter().count()
          - startingSerializedSizeBytes).isEqualTo(expectedSerializedSize);

      log.info("Messages received: " + receivedCount.get());
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }

    assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(testProperties.getTestTopic()).size()).isEqualTo(1);
  }
}
