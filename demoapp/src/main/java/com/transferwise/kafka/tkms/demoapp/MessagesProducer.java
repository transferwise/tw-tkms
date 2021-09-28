package com.transferwise.kafka.tkms.demoapp;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
@Slf4j
public class MessagesProducer {

  @Autowired
  private ITransactionalKafkaMessageSender tkms;

  @Autowired
  private TransactionTemplate transactionTemplate;

  @Autowired
  private PaceTracker paceTracker;

  @SneakyThrows
  public void produce(long threadCount, long batchCount, long batchSize) {
    paceTracker.startTracking();

    StringBuilder sb = new StringBuilder("Hello World!");

    int additionalMessageBytes = 1000;
    for (int i = 0; i < additionalMessageBytes; i++) {
      sb.append((char) ('a' + ThreadLocalRandom.current().nextInt(26)));
    }

    String textMessage = sb.toString();

    Thread[] threads = new Thread[(int) threadCount];
    for (long t = 0; t < threadCount; t++) {
      long finalT = t;
      threads[(int) t] = new Thread(() -> {
        try {
          for (long i = 0; i < batchCount; i++) {
            long finalI = i;
            transactionTemplate.executeWithoutResult(status -> {
              for (long j = 0; j < batchSize; j++) {
                String key = String.valueOf(finalT * batchCount * batchSize + finalI * batchSize + j);

                TkmsMessage message = new TkmsMessage()
                    .setTopic("MyTopic")
                    .setTimestamp(Instant.now())
                    .setKey(key).setValue(textMessage.getBytes(StandardCharsets.UTF_8));

                tkms.sendMessage(message);
                paceTracker.messagesInserted(1);
                ExceptionUtils.doUnchecked(() -> {
                  // Can be used to check if keeping transactions open is creating any long locks somewhere.
                  // Thread.sleep(10000);
                });
              }
            });
          }
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
      });
    }

    for (int t = 0; t < threadCount; t++) {
      threads[t].start();
    }

    for (int t = 0; t < threadCount; t++) {
      threads[t].join();
    }
  }
}
