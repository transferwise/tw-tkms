package com.transferwise.kafka.tkms.demoapp;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MessagesListener {

  private AtomicLong i = new AtomicLong();

  @KafkaListener(topics = "MyTopic")
  @SneakyThrows
  public void listen(ConsumerRecord<String, byte[]> consumerRecord) {
    if (i.incrementAndGet() % 10000 == -1) {
      log.info("i: " + i);
      log.info("Received " + consumerRecord.key());
      log.info("Latency: " + (Instant.now().toEpochMilli() - consumerRecord.timestamp()));
    }
  }
}
