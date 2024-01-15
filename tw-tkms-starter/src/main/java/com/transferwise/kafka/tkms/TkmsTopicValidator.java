package com.transferwise.kafka.tkms;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.kafka.tkms.TransactionalKafkaMessageSender.FetchTopicDescriptionRequest;
import com.transferwise.kafka.tkms.TransactionalKafkaMessageSender.FetchTopicDescriptionResponse;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsKafkaAdminProvider;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationLevel;
import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationType;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TkmsTopicValidator implements ITkmsTopicValidator, InitializingBean {

  @Autowired
  private IExecutorServicesProvider executorServicesProvider;

  @Autowired
  private MeterRegistry meterRegistry;

  @Autowired
  private TkmsProperties tkmsProperties;

  @Autowired
  protected ITkmsKafkaAdminProvider tkmsKafkaAdminProvider;

  @Autowired
  protected IProblemNotifier problemNotifier;

  @Autowired
  protected ITkmsKafkaProducerProvider tkmsKafkaProducerProvider;

  @Autowired
  protected ITkmsMetricsTemplate tkmsMetricsTemplate;

  private LoadingCache<FetchTopicDescriptionRequest, FetchTopicDescriptionResponse> topicDescriptionsCache;

  private final Map<String, Boolean> topicsValidatedDuringInitializationOrNotified = new ConcurrentHashMap<>();

  private ExecutorService executor;

  public void afterPropertiesSet() {
    this.executor = new ThreadNamingExecutorServiceWrapper("tw-tkms-td-cache", executorServicesProvider.getGlobalExecutorService());
    topicDescriptionsCache = Caffeine.newBuilder()
        .maximumSize(10_000)
        .executor(executor)
        .expireAfterWrite(Duration.ofMinutes(5))
        .refreshAfterWrite(Duration.ofSeconds(30))
        .recordStats()
        .build(this::fetchTopicDescription);

    CaffeineCacheMetrics.monitor(meterRegistry, topicDescriptionsCache, "tkmsTopicDescriptions");
  }

  @Override
  public void preValidateAll() {

    final var semaphore = new Semaphore(tkmsProperties.getTopicValidation().getValidationConcurrencyAtInitialization());
    final var failures = new AtomicInteger();
    final var phaser = new Phaser(1);
    final var startTimeEpochMs = System.currentTimeMillis();

    for (int shard = 0; shard < tkmsProperties.getShardsCount(); shard++) {
      var shardProperties = tkmsProperties.getShards().get(shard);
      List<String> shardTopics = shardProperties == null ? null : shardProperties.getTopics();

      if (shardTopics != null && shard == tkmsProperties.getDefaultShard()) {
        throw new IllegalStateException("Topics for default shard have to be specified on 'tw-tkms.topics' property.");
      }

      if (shard == tkmsProperties.getDefaultShard()) {
        shardTopics = tkmsProperties.getTopics();
      }

      if (shardTopics == null) {
        continue;
      }

      for (var topic : shardTopics) {
        topicsValidatedDuringInitializationOrNotified.put(topic, Boolean.TRUE);
        final var timeoutMs =
            tkmsProperties.getTopicValidation().getTopicPreValidationTimeout().toMillis() - System.currentTimeMillis() + startTimeEpochMs;
        if (ExceptionUtils.doUnchecked(() -> semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS))) {

          final var finalShard = shard;
          /*
            We are validating one by one, to get the proper error messages from Kafka.
            And, we are doing it concurrently to speed things up.
           */
          phaser.register();
          executor.execute(() -> {
            try {
              validate(TkmsShardPartition.of(finalShard, 0), topic, null);

              log.info("Topic '{}' successfully pre-validated.", topic);
            } catch (Throwable t) {
              log.error("Topic validation for '" + topic + "' failed.", t);
              failures.incrementAndGet();
            } finally {
              phaser.arriveAndDeregister();
              semaphore.release();
            }
          });
        } else {
          break;
        }
      }
    }

    final var timeoutMs =
        tkmsProperties.getTopicValidation().getTopicPreValidationTimeout().toMillis() - System.currentTimeMillis() + startTimeEpochMs;

    int phase = phaser.arrive();
    try {
      phaser.awaitAdvanceInterruptibly(phase, timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException e) {
      tkmsKafkaProducerProvider.closeKafkaProducersForTopicValidation();
      throw new IllegalStateException("Topic validation is taking too long.");
    }

    if (failures.get() > 0) {
      tkmsKafkaProducerProvider.closeKafkaProducersForTopicValidation();
      throw new IllegalStateException("There were failures with topics validations. Refusing to start.");
    }
  }

  @Override
  public void validate(TkmsShardPartition shardPartition, String topic, Integer partition) {
    if (tkmsProperties.getTopicValidation().isUseAdminClient()) {
      validateUsingAdmin(shardPartition, topic, partition);
    } else {
      validateUsingProducer(shardPartition, topic);
    }
  }

  protected void validateUsingAdmin(TkmsShardPartition shardPartition, String topic, Integer partition) {
    topicsValidatedDuringInitializationOrNotified.computeIfAbsent(topic, k -> {
      problemNotifier.notify(shardPartition.getShard(), NotificationType.TOPIC_NOT_VALIDATED_AT_INIT, NotificationLevel.WARN, () ->
          "Topic '" + topic + "' was not validated during initialization. This can introduce some lag."
              + " Please specify all the topics this service is using in the Tkms property of 'topics'."
      );
      return Boolean.TRUE;
    });

    var response = topicDescriptionsCache.get(new FetchTopicDescriptionRequest()
        .setTopic(topic)
        .setShardPartition(shardPartition)
    );

    if (response == null) {
      throw new NullPointerException("Could not fetch topic description for topic '" + topic + "'.");
    }

    if (response.getThrowable() != null) {
      String message;
      if (response.getThrowable() instanceof UnknownTopicOrPartitionException) {
        message = "Topic '" + topic + "' does not exist.";
      } else {
        message = "Topic validation for '" + topic + "' failed.";
      }
      throw new IllegalStateException(message, response.getThrowable());
    }

    final var topicDescription = response.getTopicDescription();

    final var aclOperations = topicDescription.authorizedOperations();
    if (aclOperations == null || aclOperations.isEmpty()) {
      tkmsMetricsTemplate.registerNoAclOperationsFetched(shardPartition, topic);
    } else if (!aclOperations.contains(AclOperation.ALL)
        && !aclOperations.contains(AclOperation.WRITE)
    ) {
      throw new IllegalStateException("The service does not have any ACLs of ALL/WRITE/IDEMPOTENT_WRITE on topic '" + topic + "'."
          + " The ACLs available are '" + StringUtils.join(aclOperations, ",") + "'.");
    }

    if (partition != null) {
      if (topicDescription.partitions().size() < partition - 1) {
        throw new IllegalStateException("Kafka partition " + partition + " does not exist for topic '" + topic + "'.");
      }
    }
  }

  /*
    Legacy logic.
    We keep it in, in case some service would run into issues with the admin client based validation.
   */
  protected void validateUsingProducer(TkmsShardPartition shardPartition, String topic) {
    tkmsKafkaProducerProvider.getKafkaProducerForTopicValidation(shardPartition).partitionsFor(topic);
  }

  protected FetchTopicDescriptionResponse fetchTopicDescription(FetchTopicDescriptionRequest request) {
    final var result = fetchTopicDescription0(request);

    if (result.getThrowable() != null
        && result.getThrowable() instanceof UnknownTopicOrPartitionException
        && tkmsProperties.getTopicValidation().isTryToAutoCreateTopics()) {
      final var topic = request.getTopic();
      final var shardPartition = request.getShardPartition();

      try {
        validateUsingProducer(shardPartition, topic);

        log.info("Succeeded in auto creating topic `{}`", topic);

        return fetchTopicDescription0(request);
      } catch (Throwable t) {
        log.warn("Trying to auto create topic `{}` failed.", topic, t);
        // Close the producer, so it would not spam the metadata fetch failures forever.
        tkmsKafkaProducerProvider.closeKafkaProducerForTopicValidation(shardPartition);
      }
    }

    return result;
  }

  protected FetchTopicDescriptionResponse fetchTopicDescription0(FetchTopicDescriptionRequest request) {
    final var topic = request.getTopic();
    TopicDescription topicDescription = null;

    Throwable throwable = null;

    try {
      topicDescription = tkmsKafkaAdminProvider.getKafkaAdmin(request.getShardPartition()).describeTopics(Collections.singleton(topic),
          new DescribeTopicsOptions().includeAuthorizedOperations(true)).allTopicNames().get(30, TimeUnit.SECONDS).get(topic);
    } catch (Throwable t) {
      if (t instanceof ExecutionException) {
        throwable = t.getCause();
      } else {
        throwable = t;
      }
    }

    if (throwable != null) {
      return new FetchTopicDescriptionResponse().setThrowable(throwable);
    }

    return new FetchTopicDescriptionResponse().setTopicDescription(topicDescription);
  }
}
