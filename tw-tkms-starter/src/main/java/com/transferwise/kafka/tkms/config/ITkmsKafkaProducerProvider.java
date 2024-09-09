package com.transferwise.kafka.tkms.config;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import org.apache.kafka.clients.producer.Producer;

public interface ITkmsKafkaProducerProvider {

  Producer<String, byte[]> getKafkaProducer(TkmsShardPartition tkmsShardPartition, UseCase useCase);

  Producer<String, byte[]> getKafkaProducerForTopicValidation(TkmsShardPartition shardPartition);

  void closeKafkaProducer(TkmsShardPartition tkmsShardPartition, UseCase useCase);

  void closeKafkaProducerForTopicValidation(TkmsShardPartition tkmsShardPartition);

  void closeKafkaProducersForTopicValidation();

  enum UseCase {
    PROXY,
    TEST,
    TOPIC_VALIDATION
  }
}
