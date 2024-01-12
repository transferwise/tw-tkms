package com.transferwise.kafka.tkms.config;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import org.apache.kafka.clients.producer.KafkaProducer;

public interface ITkmsKafkaProducerProvider {

  KafkaProducer<String, byte[]> getKafkaProducer(TkmsShardPartition tkmsShardPartition, UseCase useCase);

  KafkaProducer<String, byte[]> getKafkaProducerForTopicValidation(TkmsShardPartition shardPartition);

  void closeKafkaProducer(TkmsShardPartition tkmsShardPartition, UseCase useCase);

  void closeKafkaProducerForTopicValidation();

  enum UseCase {
    PROXY,
    TEST,
    TOPIC_VALIDATION
  }
}
