package com.transferwise.kafka.tkms.demoapp;

import java.util.Arrays;
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
  public void init() {
    try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig())) {
      try {
        CreateTopicsResult result = adminClient.createTopics(Arrays.asList(new NewTopic("MyTopic", 10, (short) 1)));
        result.all().get();
      } catch (Exception e) {
        Map<String, NewPartitions> map = new HashMap<>();
        map.put("MyTopic", NewPartitions.increaseTo(10));
        try {
          adminClient.createPartitions(map).all().get();
        } catch (Exception e1) {

        }
      }
    }
  }
}
