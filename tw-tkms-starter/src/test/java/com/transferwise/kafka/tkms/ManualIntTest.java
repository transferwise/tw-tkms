package com.transferwise.kafka.tkms;

import static org.awaitility.Awaitility.await;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@TestInstance(Lifecycle.PER_CLASS)
@ActiveProfiles(profiles = {"test", "postgres"})
@Disabled("Manually executed tests and utilities")
class ManualIntTest extends BaseIntTest {

  @Autowired
  private TkmsStorageToKafkaProxy tkmsStorageToKafkaProxy;

  @Autowired
  private ITransactionsHelper transactionsHelper;

  @Autowired
  private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;

  @BeforeAll
  public void setupClass() {
    tkmsStorageToKafkaProxy.pause();
    await().until(() -> tkmsStorageToKafkaProxy.isPaused());
  }

  @AfterEach
  public void cleanupClass() {
    tkmsRegisteredMessagesCollector.enable();
  }

  @Test
  @SneakyThrows
  void waitLongTime() {
    tkmsStorageToKafkaProxy.resume();

    Thread.sleep(6_000_000);
  }

  @Test
  @SneakyThrows
  void fillTkmsTable() {
    tkmsRegisteredMessagesCollector.disable();

    var content = "Hello World!";
    var contentBytes = content.getBytes(StandardCharsets.UTF_8);

    var batches = 100;
    var batchSize = 100_000;
    final var startTimeMs = System.currentTimeMillis();

    var executors = Executors.newFixedThreadPool(25);

    for (int t = 0; t < batches; t++) {
      int finalT = t;
      executors.submit(() -> {
        try {
          transactionsHelper.withTransaction().run(() -> {
            for (int i = 0; i < batchSize; i++) {
              transactionalKafkaMessageSender.sendMessage(new TkmsMessage().setTopic("TestTopicPostgres").setKey("a").setValue(contentBytes));
            }
          });
          log.info("Inserted batch #" + finalT);
        } catch (Throwable throwable) {
          log.error(throwable.getMessage(), throwable);
        }
      });
    }

    executors.shutdown();
    executors.awaitTermination(10, TimeUnit.MINUTES);

    log.info("Added {} records in {} ms.", batches * batchSize, System.currentTimeMillis() - startTimeMs);
  }

}
