package com.transferwise.kafka.tkms.demoapp;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MessagesListener {

  @Autowired
  private MeterRegistry meterRegistry;

  @KafkaListener(topics = "MyTopic")
  @SneakyThrows
  public void listen(ConsumerRecord<String, byte[]> consumerRecord) {
    meterRegistry.timer("tw.tkms.demoapp.messages.received").record(Instant.now().toEpochMilli() - consumerRecord.timestamp(), TimeUnit.MILLISECONDS);
  }
}
