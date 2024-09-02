package com.transferwise.kafka.tkms.config;

import java.util.function.Function;
import org.apache.kafka.clients.producer.Producer;

public interface ITkmsKafkaProducerPostProcessor extends Function<Producer<String, byte[]>, Producer<String, byte[]>> {
}
