package com.transferwise.kafka.tkms.demoapp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConfiguration {

  @Autowired
  private KafkaAdmin kafkaAdmin;

  @PostConstruct
  public void inits() {
    try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig())) {
      createTopic(adminClient, new NewTopic("MyTopic", 10, (short) 1));
      createTopic(adminClient, new NewTopic("ComplexTest0", 10, (short) 1));
      createTopic(adminClient, new NewTopic("ComplexTest1", 10, (short) 1));
      createTopic(adminClient, new NewTopic("ComplexTest2", 10, (short) 1));
      createTopic(adminClient, new NewTopic("ComplexTest3", 10, (short) 1));
      createTopic(adminClient, new NewTopic("ComplexTest4", 10, (short) 1));
    }
  }

  protected void createTopic(AdminClient adminClient, NewTopic newTopic) {
    try {
      CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
      result.all().get();
    } catch (InterruptedException | ExecutionException e) {
      increasePartitions(adminClient, newTopic.name(), newTopic.numPartitions());
    }
  }

  protected void increasePartitions(AdminClient adminClient, String topic, int numPartitions) {
    try {
      Map<String, NewPartitions> map = new HashMap<>();
      map.put(topic, NewPartitions.increaseTo(numPartitions));
      adminClient.createPartitions(map).all().get();
    } catch (Throwable ignored) {
      // ignored
    }
  }
}
