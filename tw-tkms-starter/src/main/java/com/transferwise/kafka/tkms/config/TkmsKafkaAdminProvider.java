package com.transferwise.kafka.tkms.config;

import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties.ShardProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TkmsKafkaAdminProvider implements ITkmsKafkaAdminProvider, GracefulShutdownStrategy {

  private static final Set<String> CONFIG_NAMES = AdminClientConfig.configNames();
  /**
   * Keep the kafka-clients' MBean registration happy.
   */
  private static final AtomicInteger sequence = new AtomicInteger();

  @Autowired
  private TkmsProperties tkmsProperties;

  @Autowired
  private MeterRegistry meterRegistry;

  private Map<TkmsShardPartition, AdminEntry> admins = new ConcurrentHashMap<>();

  @Override
  public Admin getKafkaAdmin(TkmsShardPartition tkmsShardPartition) {
    return admins.computeIfAbsent(tkmsShardPartition, key -> {
      var configs = new HashMap<String, Object>();

      configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "Please specify 'tw-tkms.kafka.bootstrap.servers'.");
      configs.put(AdminClientConfig.CLIENT_ID_CONFIG, "tw-tkms-topic-validation-" + sequence.incrementAndGet());
      configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
      configs.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, 100);
      configs.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 5000);

      for (var e : tkmsProperties.getKafka().entrySet()) {
        if (CONFIG_NAMES.contains(e.getKey())) {
          configs.put(e.getKey(), e.getValue());
        }
      }

      ShardProperties shardProperties = tkmsProperties.getShards().get(tkmsShardPartition.getShard());
      if (shardProperties != null) {
        for (var e : shardProperties.getKafka().entrySet()) {
          if (CONFIG_NAMES.contains(e.getKey())) {
            configs.put(e.getKey(), e.getValue());
          }
        }
      }

      final var admin = KafkaAdminClient.create(configs);
      final var kafkaClientMetrics = new KafkaClientMetrics(admin);
      kafkaClientMetrics.bindTo(meterRegistry);

      return new AdminEntry().setAdmin(admin).setKafkaClientMetric(kafkaClientMetrics);
    }).getAdmin();
  }

  @Override
  public void closeKafkaAdmin(TkmsShardPartition tkmsShardPartition) {
    var adminEntry = admins.remove(tkmsShardPartition);

    if (adminEntry == null) {
      return;
    }

    adminEntry.getKafkaClientMetric().close();

    try {
      adminEntry.getAdmin().close(Duration.ofSeconds(5));
    } catch (Throwable t) {
      log.error("Closing Kafka admin failed.", t);
    }
  }

  @Override
  public void applicationTerminating() {
    admins.keySet().forEach((this::closeKafkaAdmin));
  }

  @Override
  public boolean canShutdown() {
    return true;
  }

  @Data
  @Accessors(chain = true)
  protected static class AdminEntry {

    private Admin admin;

    private KafkaClientMetrics kafkaClientMetric;
  }
}
