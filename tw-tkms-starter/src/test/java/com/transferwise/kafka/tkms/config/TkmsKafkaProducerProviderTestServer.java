package com.transferwise.kafka.tkms.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider.UseCase;
import com.transferwise.kafka.tkms.test.BaseIntTest;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import com.transferwise.kafka.tkms.test.TestKafkaProducerPostProcessor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;


class TkmsKafkaProducerProviderTestServer extends BaseIntTest {

  @Autowired
  private ITkmsKafkaProducerProvider tkmsKafkaProducerProvider;


//  @Test
  void shardKafkaPropertiesAreApplied() throws Exception {
    Producer<String, byte[]> kafkaProducer = tkmsKafkaProducerProvider.getKafkaProducer(TkmsShardPartition.of(1, 0), UseCase.PROXY);

    InvocationHandler handler = Proxy.getInvocationHandler(kafkaProducer);

    Field originalProducerFiled =  handler.getClass().getDeclaredField("producer");
    originalProducerFiled.setAccessible(true);
    Object originalProducer = originalProducerFiled.get(handler);
    Field producerConfigField = originalProducer.getClass().getDeclaredField("producerConfig");
    //    Field producerConfigField = kafkaProducer.getClass().getDeclaredField("producerConfig");
    producerConfigField.setAccessible(true);
    ProducerConfig producerConfig = (ProducerConfig) producerConfigField.get(kafkaProducer);
    ;


    assertThat(producerConfig.getLong("linger.ms")).isEqualTo(7L);
  }
}
