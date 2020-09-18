package com.transferwise.kafka.tkms.test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TestMessagesListener {

  private final List<Consumer<ConsumerRecord<String, String>>> consumers = new CopyOnWriteArrayList<>();

  @KafkaListener(topics = "${tw-tkms-test.test-topic}")
  public void retrieveMessage(ConsumerRecord<String, String> cr) {
    for (Consumer<ConsumerRecord<String, String>> consumer : consumers) {
      consumer.accept(cr);
    }
  }

  public void registerConsumer(Consumer<ConsumerRecord<String, String>> consumer) {
    consumers.add(consumer);
  }

  public void unregisterConsumer(Consumer<ConsumerRecord<String, String>> consumer) {
    consumers.remove(consumer);
  }

  @Data
  @Accessors(chain = true)
  public static class TestEvent {

    private Long id;
    private Long entityId;
    private String message;
  }

}
