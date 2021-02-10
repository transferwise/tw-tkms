package com.transferwise.kafka.tkms.config;

import org.apache.kafka.clients.producer.KafkaProducer;

public interface ITkmsKafkaProducerProvider {

  KafkaProducer<String, byte[]> getKafkaProducer(int shard);

  void closeKafkaProducer(int shard);
}
