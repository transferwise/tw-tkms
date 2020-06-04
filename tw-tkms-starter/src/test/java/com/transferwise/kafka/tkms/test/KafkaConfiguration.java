package com.transferwise.kafka.tkms.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
  @Autowired
  private TestProperties tkmsProperties;

  @PostConstruct
  public void init() {
    try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig())) {
      try {
        CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(new NewTopic(tkmsProperties.getTestTopic(), 10, (short) 1)));
        result.all().get();
      } catch (Throwable t) {
        Map<String, NewPartitions> map = new HashMap<>();
        map.put(tkmsProperties.getTestTopic(), NewPartitions.increaseTo(10));
        try {
          adminClient.createPartitions(map).all().get();
        } catch (Throwable ignored) {
          //ignored
        }
      }
    }
  }
}
