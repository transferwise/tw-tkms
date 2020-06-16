package com.transferwise.kafka.tkms.config;

import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.kafka.tkms.config.TkmsProperties.ShardProperties;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TkmsKafkaProducerProvider implements ITkmsKafkaProducerProvider, GracefulShutdownStrategy {

  @Autowired
  private TkmsProperties tkmsProperties;

  private Map<Integer, KafkaProducer<String, byte[]>> kafkaProducers = new ConcurrentHashMap<>();

  @Override
  public KafkaProducer<String, byte[]> getKafkaProducer(int shard) {
    return kafkaProducers.computeIfAbsent(shard, key -> {
      Map<String, Object> configs = new HashMap<>();

      configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      configs.put(ProducerConfig.ACKS_CONFIG, "all");
      configs.put(ProducerConfig.RETRIES_CONFIG, "0");
      configs.put(ProducerConfig.BATCH_SIZE_CONFIG, "163840");
      configs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760");
      configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 50);
      configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Please specify tw-tkms.kafka.bootstrap.servers");
      configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
      configs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");

      configs.putAll(tkmsProperties.getKafka());

      ShardProperties shardProperties = tkmsProperties.getShards().get(shard);
      if (shardProperties != null) {
        configs.putAll(tkmsProperties.getKafka());
      }

      return new KafkaProducer<>(configs);
    });
  }

  @Override
  public void closeKafkaProducer(int shard) {
    KafkaProducer<String, byte[]> producer = kafkaProducers.remove(shard);
    if (producer != null) {
      try {
        producer.close(Duration.ofSeconds(5));
      } catch (Throwable t) {
        log.error(t.getMessage(), t);
      }
    }
  }

  @Override
  public void applicationTerminating() {
    kafkaProducers.forEach((shard, producer) -> {
      try {
        producer.close(Duration.ofSeconds(5));
      } catch (Throwable t) {
        log.error(t.getMessage(), t);
      }
    });
  }

  @Override
  public boolean canShutdown() {
    return true;
  }
}
