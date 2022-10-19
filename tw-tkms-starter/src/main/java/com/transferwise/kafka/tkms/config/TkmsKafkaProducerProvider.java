package com.transferwise.kafka.tkms.config;

import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.kafka.tkms.config.TkmsProperties.ShardProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
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

  @Autowired
  private MeterRegistry meterRegistry;

  private Map<Integer, KafkaProducer<String, byte[]>> kafkaProducers = new ConcurrentHashMap<>();

  private Map<Integer, KafkaClientMetrics> kafkaClientMetrics = new HashMap<>();

  @Override
  public KafkaProducer<String, byte[]> getKafkaProducer(int shard) {
    return kafkaProducers.computeIfAbsent(shard, key -> {
      Map<String, Object> configs = new HashMap<>();

      configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      configs.put(ProducerConfig.ACKS_CONFIG, "all");
      configs.put(ProducerConfig.BATCH_SIZE_CONFIG, "163840");
      configs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(tkmsProperties.getMaximumMessageBytes()));
      configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
      configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Please specify 'tw-tkms.kafka.bootstrap.servers'.");
      configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
      configs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
      configs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
      configs.put(ProducerConfig.LINGER_MS_CONFIG, "5");

      configs.putAll(tkmsProperties.getKafka());

      ShardProperties shardProperties = tkmsProperties.getShards().get(shard);
      if (shardProperties != null) {
        configs.putAll(shardProperties.getKafka());
      }

      KafkaProducer<String, byte[]> kafkaProducer = new KafkaProducer<>(configs);
      kafkaClientMetrics.put(shard, new KafkaClientMetrics(kafkaProducer));
      kafkaClientMetrics.get(shard).bindTo(meterRegistry);
      return kafkaProducer;
    });
  }

  @Override
  public void closeKafkaProducer(int shard) {
    KafkaClientMetrics kafkaClientMetric = kafkaClientMetrics.remove(shard);
    if (kafkaClientMetric != null) {
      kafkaClientMetric.close();
    }

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
      KafkaClientMetrics kafkaClientMetric = kafkaClientMetrics.remove(shard);
      if (kafkaClientMetric != null) {
        kafkaClientMetric.close();
      }

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
