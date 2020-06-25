package com.transferwise.kafka.tkms.test;

import com.transferwise.common.baseutils.ExceptionUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
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
  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
  public void init() {
    try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig())) {
      deleteTopic(adminClient, tkmsProperties.getTestTopic());
    }
    try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfig())) {
      createTopic(adminClient, tkmsProperties.getTestTopic(), 10);
    }
  }

  protected void deleteTopic(AdminClient adminClient, String topicName) {
    try {
      final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singleton(topicName));
      deleteTopicsResult.values().get(topicName).get();

      log.info("Deleted Kafka topic '" + topicName + "'.");
    } catch (InterruptedException | ExecutionException e) {
      if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  protected void createTopic(final AdminClient adminClient, final String topicName, final int partitions) {
    // It sometimes takes time for delete to actually apply and finalize.
    for (int i = 0; i < 50; i++) {
      try {
        final NewTopic newTopic = new NewTopic(topicName, partitions, (short) 1);
        final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
        createTopicsResult.values().get(topicName).get();
        log.info("Created Kafka topic '" + topicName + "' in " + i + ".");
        return;
      } catch (InterruptedException | ExecutionException e) {
        if (!(e.getCause() instanceof TopicExistsException)) {
          throw new RuntimeException(e.getMessage(), e);
        }
        ExceptionUtils.doUnchecked(() -> Thread.sleep(5));
      }
    }
  }

}
