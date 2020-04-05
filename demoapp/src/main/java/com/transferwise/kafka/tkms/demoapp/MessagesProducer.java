package com.transferwise.kafka.tkms.demoapp;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.api.Message;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

  @SneakyThrows
  public void produce(long threadCount, long batchCount, long batchSize) {
    long startTimeMs = System.currentTimeMillis();
    AtomicLong processedCount = new AtomicLong();

    @SuppressWarnings("StringBufferReplaceableByString") StringBuilder sb = new StringBuilder("Hello World!");

    /*
    for (int i = 0; i < 2048; i++) {
      sb.append((char)('a' + ThreadLocalRandom.current().nextInt(26)));
    }*/
    String textMessage = sb.toString();

    Thread[] threads = new Thread[(int) threadCount];
    for (long t = 0; t < threadCount; t++) {
      long finalT = t;
      threads[(int) t] = new Thread(() -> {
        try {
          for (long i = 0; i < batchCount; i++) {
            long finalI = i;
            transactionTemplate.executeWithoutResult((status) -> {
              for (long j = 0; j < batchSize; j++) {
                String key = String.valueOf(finalT * batchCount * batchSize + finalI * batchSize + j);

                Message message = new Message()
                    .setTopic("MyTopic")
                    .setTimestamp(Instant.now())
                    .setKey(key).setValue(textMessage.getBytes(StandardCharsets.UTF_8));

                tkms.sendMessage(message);
                processedCount.incrementAndGet();
              }
            });
          }
        }
        catch (Exception e){
          log.error(e.getMessage(), e);
        }
      });
    }

    AtomicBoolean stopTracking = new AtomicBoolean();
    Thread trackerThread = new Thread(() -> {
      int i = 0;
      while (!stopTracking.get()) {
        if (i++ % 100 == 0) {
          long cnt = processedCount.get();
          if (cnt > 0) {
            long endTimeMs = System.currentTimeMillis();
            log.info("Time taken for " + cnt + " messages: " + (endTimeMs - startTimeMs) + " ms.");
            log.info("Pace: " + (cnt * 1000 / (endTimeMs - startTimeMs)) + " messages/s");
          }
        }
        ExceptionUtils.doUnchecked(() -> Thread.sleep(100));
      }
    });
    trackerThread.start();

    for (int t = 0; t < threadCount; t++) {
      threads[t].start();
    }

    for (int t = 0; t < threadCount; t++) {
      threads[t].join();
    }

    stopTracking.set(true);
  }
}
