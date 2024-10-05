package com.transferwise.kafka.tkms;

import static com.transferwise.common.baseutils.UuidUtils.generatePrefixCombUuid;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender.SendMessageRequest;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender.SendMessagesRequest;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender.SendMessagesResult;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsMessage.Compression;
import com.transferwise.kafka.tkms.api.TkmsMessage.Header;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsDaoProvider;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider;
import com.transferwise.kafka.tkms.dao.FaultInjectedTkmsDao;
import com.transferwise.kafka.tkms.metrics.TkmsMetricsTemplate;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import com.transferwise.kafka.tkms.test.BaseTestEnvironment;
import com.transferwise.kafka.tkms.test.TestMessagesListener;
import com.transferwise.kafka.tkms.test.TestMessagesListener.TestEvent;
import com.transferwise.kafka.tkms.test.TestProperties;
import io.micrometer.core.instrument.Counter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;

@BaseTestEnvironment
@Slf4j
abstract class EndToEndIntTest extends BaseIntTest {

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
  private ITkmsDaoProvider tkmsDaoProvider;
  @Autowired
  private TkmsStorageToKafkaProxy tkmsStorageToKafkaProxy;
  @Autowired
  private ITkmsKafkaProducerProvider tkmsKafkaProducerProvider;

  private FaultInjectedTkmsDao faultInjectedTkmsDao;

  @BeforeEach
  public void setup() {
    var tkmsDao = tkmsDaoProvider.getTkmsDao(0);
    faultInjectedTkmsDao = new FaultInjectedTkmsDao(tkmsDao);
    tkmsStorageToKafkaProxy.setTkmsDaoProvider(shard -> faultInjectedTkmsDao);
    ((TransactionalKafkaMessageSender) transactionalKafkaMessageSender).setTkmsDaoProvider(shard -> faultInjectedTkmsDao);

    super.setup();
  }

  @AfterEach
  public void cleanup() {
    super.cleanup();
    tkmsStorageToKafkaProxy.setTkmsDaoProvider(tkmsDaoProvider);
    ((TransactionalKafkaMessageSender) transactionalKafkaMessageSender).setTkmsDaoProvider(tkmsDaoProvider);
    tkmsProperties.setDeferMessageRegistrationUntilCommit(false);
    tkmsProperties.setValidateSerialization(false);
    tkmsProperties.getTopicValidation().setUseAdminClient(false);
  }

  protected void setupConfig(boolean deferUntilCommit) {
    tkmsProperties.setDeferMessageRegistrationUntilCommit(deferUntilCommit);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SneakyThrows
  void testThatJsonStringMessageCanBeSentAndRetrieved(boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);

    var messagePart = "Hello World!";
    int messageMultiplier = 100;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < messageMultiplier; i++) {
      sb.append(messagePart);
    }
    var uuid = generatePrefixCombUuid();
    var priority = 17L;

    tkmsStorageToKafkaProxy.pause();

    String message = sb.toString();

    AtomicInteger receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> ExceptionUtils.doUnchecked(() -> {
      TestEvent receivedEvent = objectMapper.readValue(cr.value(), TestEvent.class);
      if (receivedEvent.getMessage().equals(message)) {
        assertThat(cr.headers().toArray()).hasSize(3);

        org.apache.kafka.common.header.Header criticalityHeader = cr.headers().toArray()[0];
        assertThat(criticalityHeader.key()).isEqualTo("x-tw-criticality");
        assertThat(new String(criticalityHeader.value(), StandardCharsets.UTF_8)).isEqualTo("PrettyLowLol");

        org.apache.kafka.common.header.Header priorityHeader = cr.headers().toArray()[1];
        assertThat(priorityHeader.key()).isEqualTo(StandardHeaders.X_WISE_PRIORITY);
        assertThat(Long.parseLong(new String(priorityHeader.value(), StandardCharsets.UTF_8))).isEqualTo(priority);

        org.apache.kafka.common.header.Header uuidHeader = cr.headers().toArray()[2];
        assertThat(uuidHeader.key()).isEqualTo(StandardHeaders.X_WISE_UUID);
        assertThat(UUID.fromString(new String(uuidHeader.value(), StandardCharsets.UTF_8))).isEqualTo(uuid);

        receivedCount.incrementAndGet();
      } else {
        throw new IllegalStateException("Wrong message receive: " + receivedEvent.getMessage());
      }
    });

    testMessagesListener.registerConsumer(messageCounter);
    try {
      var testEvent = new TestEvent().setId(1L).setMessage(message);

      await().until(() -> tkmsStorageToKafkaProxy.isPaused());

      transactionsHelper.withTransaction().run(() -> {
        var result = transactionalKafkaMessageSender.sendMessage(
            new TkmsMessage()
                .setUuid(uuid)
                .setPriority(priority)
                .setTopic(testProperties.getTestTopic())
                .setValue(toJsonBytes(testEvent))
                .addHeader(
                    new Header()
                        .setKey("x-tw-criticality")
                        .setValue("PrettyLowLol".getBytes(StandardCharsets.UTF_8))
                )
        );

        var messagesCount = tkmsTestDao.getMessagesCount(result.getShardPartition());
        if (deferUntilCommit) {
          // Counter is incremented when the record gets later inserted
          assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(testProperties.getTestTopic()).size()).isEqualTo(0);
          // Messages are not in the database yet.
          assertThat(messagesCount).isZero();
        } else {
          assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(testProperties.getTestTopic()).size()).isEqualTo(1);
          assertThat(messagesCount).isEqualTo(1);
        }
      });

      tkmsStorageToKafkaProxy.resume();

      await().until(() -> receivedCount.get() > 0);

      log.info("Messages received: " + receivedCount.get());
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }

    waitUntilTablesAreEmpty();

    assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(testProperties.getTestTopic()).size()).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3})
  void testExactlyOnceDelivery(int scenario) throws Exception {
    var deferUntilCommit = scenario == 0 || scenario == 2;
    var validateSerialization = scenario == 1 || scenario == 3;

    setupConfig(deferUntilCommit);
    tkmsProperties.setValidateSerialization(validateSerialization);

    // For producer to create more batches and spread messages around different partitions.
    String message = StringUtils.repeat("Hello World!", 100);
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
                transactionalKafkaMessageSender.sendMessage(
                    new TkmsMessage()
                        .setUuid(generatePrefixCombUuid())
                        .setTopic(testProperties.getTestTopic())
                        .setValue(objectMapper.writeValueAsBytes(testEvent))
                );
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
      assertThat(partitionsMap.entrySet().size()).as("At least some partitions received messages").isGreaterThanOrEqualTo(2);
      partitionsMap.forEach((key, value) -> log.info("Partition " + key + " received " + value.get() + " messages."));

      log.info("Sending " + messagesCount + " messages took " + (System.currentTimeMillis() - startTimeMs + " ms."));

      waitUntilTablesAreEmpty();
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testThatMessagesWithSameKeyEndUpInOnePartition(boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);

    String protoMessage = "Hello Estonia!";
    String message = StringUtils.repeat(protoMessage, 100);
    String key = "GrailsRocks";
    int n = 200;
    ConcurrentHashMap<Integer, AtomicInteger> partitionsMap = new ConcurrentHashMap<>();
    AtomicInteger receivedCount = new AtomicInteger();

    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> {
      var testEvent = ExceptionUtils.doUnchecked(() -> objectMapper.readValue(cr.value(), TestEvent.class));
      if (!message.equals(testEvent.getMessage())) {
        throw new IllegalStateException("Unexpected message '" + message + "' received.");
      }

      partitionsMap.computeIfAbsent(cr.partition(), (k) -> new AtomicInteger()).incrementAndGet();
      receivedCount.incrementAndGet();
    };

    testMessagesListener.registerConsumer(messageCounter);
    try {
      for (int i = 0; i < n; i++) {
        TestEvent testEvent = new TestEvent().setId(1L).setMessage(message);

        transactionsHelper.withTransaction().run(() ->
            transactionalKafkaMessageSender.sendMessage(
                new TkmsMessage()
                    .setUuid(generatePrefixCombUuid())
                    .setKey(key)
                    .setTopic(testProperties.getTestTopic())
                    .setValue(toJsonBytes(testEvent))
            )
        );
      }
      await().until(() -> receivedCount.get() >= n);

      log.info("Messages received: " + receivedCount.get());

      assertThat(partitionsMap.entrySet().size()).isEqualTo(1);

      waitUntilTablesAreEmpty();
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testThatMessagesWithSamePartitionEndUpInOnePartition(boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);

    String message = "Hello Fabio!";
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

        transactionsHelper.withTransaction().run(() ->
            transactionalKafkaMessageSender.sendMessage(
                new TkmsMessage()
                    .setUuid(generatePrefixCombUuid())
                    .setPartition(partition)
                    .setTopic(testProperties.getTestTopic())
                    .setValue(toJsonBytes(testEvent))
            )
        );
      }
      await().until(() -> receivedCount.get() >= n);

      log.info("Messages received: " + receivedCount.get());

      assertThat(partitionsMap.entrySet().size()).isEqualTo(1);
      assertThat(partitionsMap.get(partition).get()).isEqualTo(n);

      waitUntilTablesAreEmpty();
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testThatMessagesOrderForAnEntityIsPreserved(boolean deferUntilCommit) throws Exception {
    setupConfig(deferUntilCommit);

    String message = "Hello Jarvis!";
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
        long finalEntityId = e;
        threads[(int) e] = new Thread(() -> {
          for (long i = 0; i < entityEventsCount; i++) {
            long id = finalEntityId * entityEventsCount + i;
            TestEvent testEvent = new TestEvent().setId(id).setEntityId(finalEntityId).setMessage(message);
            transactionsHelper.withTransaction().run(() ->
                transactionalKafkaMessageSender.sendMessage(
                    new TkmsMessage()
                        .setUuid(generatePrefixCombUuid())
                        .setKey(String.valueOf(finalEntityId))
                        .setTopic(testProperties.getTestTopic())
                        .setValue(toJsonBytes(testEvent))
                )
            );

            checkIfTransactionContextsHaveBeenCleared();
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

      await().until(() -> receivedCount.get() >= messagesCount);

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
      waitUntilTablesAreEmpty();
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testThatMessagesOrderForAnEntityIsPreservedWithBatches(boolean deferUntilCommit) throws Exception {
    setupConfig(deferUntilCommit);

    String message = "Hello Tamas!";
    int entitiesCount = 100;
    int entityEventsCount = 100;
    int averageBatchSize = 10;
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
        long finalEntityId = e;
        threads[(int) e] = new Thread(() -> {
          int i = 0;
          while (i < entityEventsCount) {
            int batchSize = Math.min(1 + ThreadLocalRandom.current().nextInt(averageBatchSize), entityEventsCount - i);

            SendMessagesRequest sendMessagesRequest = new SendMessagesRequest();
            for (int j = 0; j < batchSize; j++) {
              var id = finalEntityId * entityEventsCount + i;
              var testEvent = new TestEvent().setId(id).setEntityId(finalEntityId).setMessage(message);
              sendMessagesRequest.addTkmsMessage(
                  new TkmsMessage()
                      .setUuid(generatePrefixCombUuid())
                      .setKey(String.valueOf(finalEntityId))
                      .setTopic(testProperties.getTestTopic())
                      .setValue(toJsonBytes(testEvent))
              );
              i++;
            }

            transactionsHelper.withTransaction().run(() ->
                transactionalKafkaMessageSender.sendMessages(sendMessagesRequest));
          }

          checkIfTransactionContextsHaveBeenCleared();
        });
      }
      for (Thread thread : threads) {
        thread.start();
      }
      final long startTimeMs = System.currentTimeMillis();
      for (Thread thread : threads) {
        thread.join();
      }

      await().until(() -> receivedCount.get() >= messagesCount);

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
      waitUntilTablesAreEmpty();
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  private static Stream<Arguments> unknownTopicsMatrix() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(false, false)
    );
  }

  @ParameterizedTest
  @MethodSource("unknownTopicsMatrix")
  @SneakyThrows
  void sendingToUnknownTopicWillBePreventedWhenTopicAutoCreationIsDisabled(boolean deferUntilCommit, boolean useAdminClient) {
    try {
      setupConfig(deferUntilCommit);
      tkmsProperties.getTopicValidation().setUseAdminClient(useAdminClient);

      var expectedMessage =
          useAdminClient ? "Topic 'NotExistingTopic' does not exist." : "Topic NotExistingTopic not present in metadata after";

      assertThatThrownBy(
          () -> transactionsHelper.withTransaction().run(
              () -> transactionalKafkaMessageSender.sendMessage(
                  new TkmsMessage()
                      .setUuid(generatePrefixCombUuid())
                      .setTopic("NotExistingTopic")
                      .setValue("Stuff".getBytes(StandardCharsets.UTF_8))
              )
          )
      ).hasMessageContaining(expectedMessage);
    } finally {
      // Stop logs spam about not existing topic in metadata.
      tkmsKafkaProducerProvider.closeKafkaProducersForTopicValidation();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sendingOutMessagesWithoutActiveTransactionsWillFail(boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);

    assertThatThrownBy(
        () -> transactionalKafkaMessageSender.sendMessage(
            new TkmsMessage()
                .setUuid(generatePrefixCombUuid())
                .setTopic("NotExistingTopic")
                .setValue("Stuff".getBytes(StandardCharsets.UTF_8))
        )
    ).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No active transaction detected.");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sendingMultipleMessagesWorks(boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);

    byte[] value = "{\"message\" : \"Hello Nerius!\"}".getBytes(StandardCharsets.UTF_8);

    AtomicInteger receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter =
        cr -> ExceptionUtils.doUnchecked(receivedCount::incrementAndGet);

    testMessagesListener.registerConsumer(messageCounter);

    String topic = testProperties.getTestTopic();

    SendMessagesResult sendMessagesResult =
        transactionsHelper.withTransaction().call(() -> transactionalKafkaMessageSender.sendMessages(new SendMessagesRequest()
            .addTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value))
            .addTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value).setShard(1))
            .addTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value).setShard(0).setPartition(0))
            .addTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value).setShard(0).setPartition(1))
        ));

    assertThat(sendMessagesResult.getResults().size()).isEqualTo(4);
    if (deferUntilCommit) {
      assertThat(sendMessagesResult.getResults().get(1).getStorageId()).isNull();
    } else {
      assertThat(sendMessagesResult.getResults().get(1).getStorageId()).isNotNull();
    }
    assertThat(sendMessagesResult.getResults().get(1).getShardPartition().getShard()).isEqualTo(1);
    assertThat(sendMessagesResult.getResults().get(2).getShardPartition().getShard()).isZero();

    try {
      await().until(() -> receivedCount.get() == 4);

      log.info("Messages received: " + receivedCount.get());
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }

    assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(topic).size()).isEqualTo(4);

    assertThat(meterRegistry.find("tw_tkms_interface_message_registration").tag("shard", "0").counter().count()).isEqualTo(3);
    assertThat(meterRegistry.find("tw_tkms_interface_message_registration").tag("shard", "1").counter().count()).isEqualTo(1);

    waitUntilTablesAreEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sendingMultipleMessagesWithMultipleStatementsWorks(boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);
    tkmsStorageToKafkaProxy.pause();

    byte[] value = "{\"message\" : \"Hello Ajmal!\"}".getBytes(StandardCharsets.UTF_8);

    AtomicInteger receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter =
        cr -> ExceptionUtils.doUnchecked(receivedCount::incrementAndGet);

    testMessagesListener.registerConsumer(messageCounter);

    String topic = testProperties.getTestTopic();

    await().until(() -> tkmsStorageToKafkaProxy.isPaused());

    transactionsHelper.withTransaction().run(() -> {
      transactionalKafkaMessageSender.sendMessages(new SendMessagesRequest()
          .addTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value))
          .addTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value).setShard(0).setPartition(0))
          .addTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value).setShard(0).setPartition(1))
          .addTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value).setShard(1))
      );

      transactionalKafkaMessageSender.sendMessage(new SendMessageRequest().setDeferMessageRegistrationUntilCommit(deferUntilCommit)
          .setTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value)));

      transactionalKafkaMessageSender.sendMessage(new SendMessageRequest()
          .setTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value)));

      transactionalKafkaMessageSender.sendMessages(new SendMessagesRequest()
          .addTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value))
          .addTkmsMessage(new TkmsMessage().setUuid(generatePrefixCombUuid()).setTopic(topic).setValue(value))
      );

      var messagesCount = tkmsTestDao.getMessagesCount(TkmsShardPartition.of(0, 0))
          + tkmsTestDao.getMessagesCount(TkmsShardPartition.of(1, 0));

      if (deferUntilCommit) {
        assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(testProperties.getTestTopic()).size()).isEqualTo(0);
        // Messages are not in the database yet.
        assertThat(messagesCount).isEqualTo(0);
      } else {
        assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(testProperties.getTestTopic()).size()).isEqualTo(8);
        assertThat(messagesCount).isEqualTo(8);
      }
    });

    tkmsStorageToKafkaProxy.resume();

    assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(topic).size()).isEqualTo(8);

    assertThat(meterRegistry.find(TkmsMetricsTemplate.SUMMARY_MESSAGES_IN_TRANSACTION).summary().count()).isEqualTo(1);
    assertThat(meterRegistry.find(TkmsMetricsTemplate.SUMMARY_MESSAGES_IN_TRANSACTION).summary().max()).isEqualTo(8);

    try {
      await().until(() -> receivedCount.get() == 8);

      log.info("Messages received: " + receivedCount.get());
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
      System.out.println("Received count: " + receivedCount);
    }

    waitUntilTablesAreEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void mixingDeferredAndNotDeferredMessagesIsPrevented(boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);

    byte[] value = "{\"message\" : \"Hello Ajmal!\"}".getBytes(StandardCharsets.UTF_8);

    String topic = testProperties.getTestTopic();

    assertThatThrownBy(() -> {
      transactionsHelper.withTransaction().run(() -> {
        transactionalKafkaMessageSender.sendMessages(
            new SendMessagesRequest().addTkmsMessage(
                new TkmsMessage()
                    .setUuid(generatePrefixCombUuid())
                    .setTopic(topic)
                    .setValue(value)
            )
        );

        transactionalKafkaMessageSender.sendMessages(
            new SendMessagesRequest()
                .setDeferMessageRegistrationUntilCommit(!deferUntilCommit)
                .addTkmsMessage(
                    new TkmsMessage()
                        .setUuid(generatePrefixCombUuid())
                        .setTopic(topic)
                        .setValue(value)
                )
        );
      });
    }).hasMessage("You can not mix deferred and not deferred messages in the same transaction, as it will break the ordering guarantees.");

    assertThatThrownBy(() -> {
      transactionsHelper.withTransaction().run(() -> {
        transactionalKafkaMessageSender.sendMessage(
            new SendMessageRequest().setTkmsMessage(
                new TkmsMessage()
                    .setUuid(generatePrefixCombUuid())
                    .setTopic(topic)
                    .setValue(value)
            )
        );

        transactionalKafkaMessageSender.sendMessage(
            new SendMessageRequest()
                .setDeferMessageRegistrationUntilCommit(!deferUntilCommit)
                .setTkmsMessage(
                    new TkmsMessage()
                        .setUuid(generatePrefixCombUuid())
                        .setTopic(topic)
                        .setValue(value)
                )
        );
      });
    }).hasMessage("You can not mix deferred and not deferred messages in the same transaction, as it will break the ordering guarantees.");

    // We can mix it between shard-partitions, because between those the order is not important.
    transactionsHelper.withTransaction().run(() -> {
      transactionalKafkaMessageSender.sendMessages(
          new SendMessagesRequest().addTkmsMessage(
              new TkmsMessage()
                  .setUuid(generatePrefixCombUuid())
                  .setTopic(topic)
                  .setValue(value)
                  .setShard(0)
          )
      );

      transactionalKafkaMessageSender.sendMessages(
          new SendMessagesRequest()
              .setDeferMessageRegistrationUntilCommit(!deferUntilCommit)
              .addTkmsMessage(
                  new TkmsMessage()
                      .setUuid(generatePrefixCombUuid())
                      .setTopic(topic)
                      .setValue(value)
                      .setShard(1)
              )
      );
    });

    transactionsHelper.withTransaction().run(() -> {
      transactionalKafkaMessageSender.sendMessage(
          new SendMessageRequest().setTkmsMessage(
              new TkmsMessage()
                  .setUuid(generatePrefixCombUuid())
                  .setTopic(topic)
                  .setValue(value)
                  .setShard(0)
          )
      );

      transactionalKafkaMessageSender.sendMessage(
          new SendMessageRequest()
              .setDeferMessageRegistrationUntilCommit(!deferUntilCommit)
              .setTkmsMessage(
                  new TkmsMessage()
                      .setUuid(generatePrefixCombUuid())
                      .setTopic(topic)
                      .setValue(value)
                      .setShard(1)
              )
      );
    });

    waitUntilTablesAreEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testThatSendingLargeMessagesWillNotCauseAnIssue(boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);

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
        throw new IllegalStateException("Wrong message received: " + receivedValue);
      }
    });

    testMessagesListener.registerConsumer(messageCounter);
    try {
      assertThatThrownBy(() ->
          transactionsHelper.withTransaction().call(() ->
              transactionalKafkaMessageSender
                  .sendMessage(
                      new TkmsMessage()
                          .setUuid(generatePrefixCombUuid())
                          .setTopic(testProperties.getTestTopic())
                          .setValue(message.getValue().getBytes(StandardCharsets.US_ASCII))
                  )
          ))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("0: Estimated message size is 10485937, which is larger than maximum of 10485760.");

      assertThatThrownBy(() ->
          transactionsHelper.withTransaction().call(() ->
              transactionalKafkaMessageSender.sendMessages(
                  new SendMessagesRequest().addTkmsMessage(
                      new TkmsMessage()
                          .setUuid(generatePrefixCombUuid())
                          .setTopic(testProperties.getTestTopic())
                          .setValue(message.getValue().getBytes(StandardCharsets.US_ASCII))
                  )
              )
          )
      )
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("0: Estimated message size is 10485937, which is larger than maximum of 10485760.");

      message.setValue(message.getValue().substring(0, 10484000));
      transactionsHelper.withTransaction().run(() ->
          transactionalKafkaMessageSender.sendMessage(
              new TkmsMessage()
                  .setUuid(generatePrefixCombUuid())
                  .setTopic(testProperties.getTestTopic())
                  .setValue(message.getValue().getBytes(StandardCharsets.US_ASCII))
          )
      );

      await().until(() -> receivedCount.get() > 0);

      log.info("Messages received: " + receivedCount.get());
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }

    assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(testProperties.getTestTopic()).size()).isEqualTo(1);

    waitUntilTablesAreEmpty();
  }

  /**
   * If `TkmsStorageToKafkaProxy has some important lines switched around "lastId" logic, the test will start failing.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testThatTemporaryDeleteFailureDoesNotLeaveTrashBehind(boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);

    String message = "Hello Peeter!";
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
        transactionsHelper.withTransaction().run(
            () -> transactionalKafkaMessageSender.sendMessage(
                new TkmsMessage()
                    .setUuid(generatePrefixCombUuid())
                    .setTopic(testProperties.getTestTopic())
                    .setValue(toJsonBytes(testEvent)))
        );
      }

      await().until(() -> receivedCount.get() >= messagesCount);
      await().until(() -> getTablesRowsCount() == messagesCount);

      faultInjectedTkmsDao.setDeleteMessagesFails(false);

      waitUntilTablesAreEmpty();

      int messagesSentToKafka = 0;
      for (var counter : meterRegistry.get("tw_tkms_proxy_message_send").tags("success", "true").counters()) {
        log.info("Counter: " + counter.getId());

        messagesSentToKafka += counter.count();
      }

      int finalMessagesSentToKafka = messagesSentToKafka;

      try {
        // Receive all messages, so they would not mess up other tests.
        await().until(() -> receivedCount.get() >= finalMessagesSentToKafka);
      } catch (RuntimeException e) {
        log.info("finalMessagesSentToKafka=" + finalMessagesSentToKafka);
        log.info("receivedCount.get()=" + receivedCount.get());
        throw e;
      }
      log.info("Messages received: " + receivedCount.get());
    } finally {
      faultInjectedTkmsDao.setDeleteMessagesFails(false);
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testThatInsertFailureDoesNotLeaveTrashBehind(boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);

    String message = "Hello Ahti!";

    var receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> ExceptionUtils.doUnchecked(() -> {
      TestEvent receivedEvent = objectMapper.readValue(cr.value(), TestEvent.class);
      if (receivedEvent.getMessage().equals(message)) {
        receivedCount.incrementAndGet();
      } else {
        throw new IllegalStateException("Wrong message receive: " + receivedEvent.getMessage());
      }
    });

    faultInjectedTkmsDao.setInsertMessagesErrorLatch(1);
    testMessagesListener.registerConsumer(messageCounter);
    try {
      var testEvent0 = new TestEvent().setId(0L).setMessage(message);
      var testEvent1 = new TestEvent().setId(1L).setMessage(message);

      assertThatThrownBy(() -> transactionsHelper.withTransaction().run(() -> {
        transactionalKafkaMessageSender.sendMessage(
            new TkmsMessage()
                .setUuid(generatePrefixCombUuid())
                .setTopic(testProperties.getTestTopic())
                .setValue(toJsonBytes(testEvent0))
                .setShard(0)
        );
        transactionalKafkaMessageSender.sendMessage(
            new TkmsMessage()
                .setUuid(generatePrefixCombUuid())
                .setTopic(testProperties.getTestTopic())
                .setValue(toJsonBytes(testEvent1))
                .setShard(1)
        );
      })).hasMessage("Haha, inserts are failing lol.");

      assertThat(receivedCount.get()).isEqualTo(0);
      // Everything got rolled back
      assertThat(getTablesRowsCount()).isEqualTo(0);

      checkIfTransactionContextsHaveBeenCleared();
    } finally {
      faultInjectedTkmsDao.setInsertMessagesErrorLatch(null);
      testMessagesListener.unregisterConsumer(messageCounter);
    }
  }

  @SneakyThrows
  protected byte[] toJsonBytes(Object value) {
    return objectMapper.writeValueAsBytes(value);
  }

  private static Stream<Arguments> compressionInput() {
    var deferUntilCommits = List.of(false, true);
    var arguments = new ArrayList<Arguments>();

    for (var deferUntilCommit : deferUntilCommits) {
      arguments.add(Arguments.of(CompressionAlgorithm.GZIP, 156, 157, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.NONE, 1218, 1218, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.LZ4, 182, 182, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.SNAPPY, 214, 214, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.SNAPPY_FRAMED, 212, 212, deferUntilCommit));
      arguments.add(Arguments.of(CompressionAlgorithm.ZSTD, 147, 147, deferUntilCommit));
    }

    return arguments.stream();
  }

  private void checkIfTransactionContextsHaveBeenCleared() {
    // We are not clearing the thread local for performance reasons
    assertThat(TransactionContext.storage.get()).isNotNull();
    assertThat(TransactionContext.storage.get().getValue()).isNull();
  }

  @ParameterizedTest
  @MethodSource("compressionInput")
  void testMessageIsCompressed(CompressionAlgorithm algorithm, int expectedSerializedSize, int expectedSerializedSizeAlt, boolean deferUntilCommit) {
    setupConfig(deferUntilCommit);

    var message = StringUtils.repeat("Hello Aivo!", 100);

    AtomicInteger receivedCount = new AtomicInteger();
    Consumer<ConsumerRecord<String, String>> messageCounter = cr -> ExceptionUtils.doUnchecked(() -> {
      TestEvent receivedEvent = objectMapper.readValue(cr.value(), TestEvent.class);
      if (receivedEvent.getMessage().equals(message)) {
        receivedCount.incrementAndGet();
      } else {
        throw new IllegalStateException("Wrong message received: " + receivedEvent.getMessage());
      }
    });

    Counter counter =
        meterRegistry.find("tw_tkms_dao_serialization_serialized_size_bytes").tag("algorithm", algorithm.name().toLowerCase()).counter();
    double startingSerializedSizeBytes = counter == null ? 0 : counter.count();

    testMessagesListener.registerConsumer(messageCounter);
    try {
      TestEvent testEvent = new TestEvent().setId(1L).setMessage(message);

      transactionsHelper.withTransaction().run(() ->
          transactionalKafkaMessageSender.sendMessage(
              new TkmsMessage()
                  .setUuid(UUID.fromString("7554ffe0-4da2-4de9-bebf-f3131fd7a84a"))
                  .setTopic(testProperties.getTestTopic())
                  .setValue(toJsonBytes(testEvent))
                  .setCompression(new Compression().setAlgorithm(algorithm))
          )
      );

      await().until(() -> receivedCount.get() > 0);
      waitUntilTablesAreEmpty();

      counter = meterRegistry.find("tw_tkms_dao_serialization_serialized_size_bytes").tag("algorithm", algorithm.name().toLowerCase()).counter();
      double serializedSizeBytes = counter == null ? 0 : counter.count();
      assertThat((int) (serializedSizeBytes - startingSerializedSizeBytes)).isIn(expectedSerializedSize, expectedSerializedSizeAlt);

      log.info("Messages received: " + receivedCount.get());
    } finally {
      testMessagesListener.unregisterConsumer(messageCounter);
    }

    assertThat(tkmsRegisteredMessagesCollector.getRegisteredMessages(testProperties.getTestTopic())).hasSize(1);
  }
}
