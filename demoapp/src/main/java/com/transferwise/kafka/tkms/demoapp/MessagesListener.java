package com.transferwise.kafka.tkms.demoapp;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.List;
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

  @Autowired
  private PaceTracker paceTracker;
  
  @KafkaListener(topics = "MyTopic", containerFactory = "kafkaListenerContainerFactory")
  @SneakyThrows
  public void listen(List<ConsumerRecord<String, byte[]>> consumerRecords) {
    for (ConsumerRecord<String, byte[]> consumerRecord : consumerRecords) {
      paceTracker.messagesDelivered(1);
      meterRegistry.timer("tw.tkms.demoapp.messages.received")
          .record(Instant.now().toEpochMilli() - consumerRecord.timestamp(), TimeUnit.MILLISECONDS);
    }
  }
}
