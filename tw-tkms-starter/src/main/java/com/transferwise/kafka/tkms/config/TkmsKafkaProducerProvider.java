package com.transferwise.kafka.tkms.config;

import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties.ShardProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

@Slf4j
public class TkmsKafkaProducerProvider implements ITkmsKafkaProducerProvider, GracefulShutdownStrategy {

  private static final Set<String> CONFIG_NAMES = ProducerConfig.configNames();

  /**
   * Keep the kafka-clients' MBean registration happy.
   */
  private static final AtomicInteger sequence = new AtomicInteger();

  @Autowired
  private TkmsProperties tkmsProperties;

  @Autowired
  private MeterRegistry meterRegistry;

  private Map<Pair<TkmsShardPartition, UseCase>, ProducerEntry> producers = new ConcurrentHashMap<>();

  private List<ITkmsKafkaProducerPostProcessor> postProcessors = new ArrayList<>();

  @Override
  public void addPostProcessor(ITkmsKafkaProducerPostProcessor postProcessor) {
    Assert.notNull(postProcessor, "'postProcessor' cannot be null");
    this.postProcessors.add(postProcessor);
  }

  @Override
  public void removePostProcessors() {
    this.postProcessors.clear();
  }

  @Override
  public Producer<String, byte[]> getKafkaProducer(TkmsShardPartition shardPartition, UseCase useCase) {
    return producers.computeIfAbsent(Pair.of(shardPartition, useCase), key -> {
      Map<String, Object> configs = new HashMap<>();

      configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      configs.put(ProducerConfig.ACKS_CONFIG, "all");
      configs.put(ProducerConfig.BATCH_SIZE_CONFIG, "163840");
      configs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(tkmsProperties.getMaximumMessageBytes()));

      // The following block is to guarantee the messages order.
      configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
      configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      configs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

      configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Please specify 'tw-tkms.kafka.bootstrap.servers'.");
      configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
      configs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
      configs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
      configs.put(ProducerConfig.CLIENT_ID_CONFIG,
          "tw-tkms-" + shardPartition.getShard() + "-" + shardPartition.getPartition() + "-" + useCase.name().toLowerCase()
              + "-" + sequence.incrementAndGet());

      if (useCase == UseCase.PROXY) {
        // We use large lingering time, because we are calling the `.flush()` anyway.
        configs.put(ProducerConfig.LINGER_MS_CONFIG, "1000");
      }
      configs.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "120000");

      for (var e : tkmsProperties.getKafka().entrySet()) {
        if (CONFIG_NAMES.contains(e.getKey())) {
          configs.put(e.getKey(), e.getValue());
        }
      }

      ShardProperties shardProperties = tkmsProperties.getShards().get(shardPartition.getShard());
      if (shardProperties != null) {
        for (var e : shardProperties.getKafka().entrySet()) {
          if (CONFIG_NAMES.contains(e.getKey())) {
            configs.put(e.getKey(), e.getValue());
          }
        }
      }

      final var producer = getKafkaProducer(configs);
      final var kafkaClientMetrics = new KafkaClientMetrics(producer);
      kafkaClientMetrics.bindTo(meterRegistry);

      return new ProducerEntry().setProducer(producer).setKafkaClientMetric(kafkaClientMetrics);
    }).getProducer();
  }

  private Producer<String, byte[]> getKafkaProducer(Map<String, Object> configs) {
    Producer<String, byte[]> producer = new KafkaProducer<>(configs);
    for (ITkmsKafkaProducerPostProcessor pp : this.postProcessors) {
      producer = pp.apply(producer);
    }
    return producer;
  }

  @Override
  public Producer<String, byte[]> getKafkaProducerForTopicValidation(TkmsShardPartition shardPartition) {
    return getKafkaProducer(TkmsShardPartition.of(shardPartition.getShard(), 0), UseCase.TOPIC_VALIDATION);
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
  public void closeKafkaProducerForTopicValidation(TkmsShardPartition tkmsShardPartition) {
    closeKafkaProducer(tkmsShardPartition, UseCase.TOPIC_VALIDATION);
  }

  @Override
  public void closeKafkaProducersForTopicValidation() {
    producers.keySet().stream().filter(key -> key.getRight() == UseCase.TOPIC_VALIDATION)
        .forEach(key -> closeKafkaProducer(key.getLeft(), key.getRight()));
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

    private Producer<String, byte[]> producer;

    private KafkaClientMetrics kafkaClientMetric;
  }
}
