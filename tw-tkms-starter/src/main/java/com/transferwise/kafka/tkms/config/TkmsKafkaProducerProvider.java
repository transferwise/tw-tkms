package com.transferwise.kafka.tkms.config;

import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties.ShardProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
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

  private Map<Pair<TkmsShardPartition, UseCase>, ProducerEntry> producers = new ConcurrentHashMap<>();

  @Override
  public KafkaProducer<String, byte[]> getKafkaProducer(TkmsShardPartition shardPartition, UseCase useCase) {
    return producers.computeIfAbsent(Pair.of(shardPartition, useCase), key -> {
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
      configs.put(ProducerConfig.CLIENT_ID_CONFIG,
          "tw-tkms-" + shardPartition.getShard() + "-" + shardPartition.getPartition() + "-" + useCase.name().toLowerCase());

      if (useCase == UseCase.PROXY) {
        // We use large lingering time, because we are calling the `.flush()` anyway.
        configs.put(ProducerConfig.LINGER_MS_CONFIG, "1000");
      }
      configs.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "120000");

      configs.putAll(tkmsProperties.getKafka());

      ShardProperties shardProperties = tkmsProperties.getShards().get(shardPartition.getShard());
      if (shardProperties != null) {
        configs.putAll(shardProperties.getKafka());
      }

      final var producer = new KafkaProducer<String, byte[]>(configs);
      final var kafkaClientMetrics = new KafkaClientMetrics(producer);
      kafkaClientMetrics.bindTo(meterRegistry);

      return new ProducerEntry().setProducer(producer).setKafkaClientMetric(kafkaClientMetrics);
    }).getProducer();
  }

  @Override
  public void closeKafkaProducer(TkmsShardPartition shardPartition, UseCase useCase) {
    var producerEntry = producers.remove(Pair.of(shardPartition, useCase));

    if (producerEntry == null) {
      return;
    }

    producerEntry.getKafkaClientMetric().close();

    try {
      producerEntry.getProducer().close(Duration.ofSeconds(5));
    } catch (Throwable t) {
      log.error("Closing Kafka producer for shard partiton " + shardPartition + " failed.", t);
    }
  }

  @Override
  public void applicationTerminating() {
    producers.keySet().forEach(key -> closeKafkaProducer(key.getLeft(), key.getRight()));
  }

  @Override
  public boolean canShutdown() {
    return true;
  }

  @Data
  @Accessors(chain = true)
  protected static class ProducerEntry {

    private KafkaProducer<String, byte[]> producer;

    private KafkaClientMetrics kafkaClientMetric;
  }
}
