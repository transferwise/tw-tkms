package com.transferwise.kafka.tkms.api;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface IMessageInterceptors {

  ProxyDecision beforeProxy(ProducerRecord<String, byte[]> producerRecord);
}
