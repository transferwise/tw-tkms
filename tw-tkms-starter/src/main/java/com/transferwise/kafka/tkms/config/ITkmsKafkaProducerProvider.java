package com.transferwise.kafka.tkms.config;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import org.apache.kafka.clients.producer.KafkaProducer;

public interface ITkmsKafkaProducerProvider {

  KafkaProducer<String, byte[]> getKafkaProducer(TkmsShardPartition tkmsShardPartition, UseCase useCase);

  void closeKafkaProducer(TkmsShardPartition tkmsShardPartition, UseCase useCase);

  enum UseCase {
    PROXY,
    TEST,
    TOPIC_VALIDATION
  }
}
