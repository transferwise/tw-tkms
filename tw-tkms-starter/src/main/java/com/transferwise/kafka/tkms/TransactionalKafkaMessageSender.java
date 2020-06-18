package com.transferwise.kafka.tkms;

import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener.MessageRegisteredEvent;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.ShardPartition;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.dao.ITkmsDao.InsertMessageResult;
import com.transferwise.kafka.tkms.metrics.IMetricsTemplate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.PostConstruct;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Transactional(rollbackFor = Exception.class)
public class TransactionalKafkaMessageSender implements ITransactionalKafkaMessageSender {

  @Autowired
  private ITkmsDao tkmsDao;
  @Autowired
  private Validator validator;
  @Autowired
  private IMetricsTemplate metricsTemplate;
  @Autowired
  private TkmsProperties properties;
  @Autowired
  private ApplicationContext applicationContext;
  @Autowired
  private ITkmsKafkaProducerProvider kafkaProducerProvider;

  private volatile List<ITkmsEventsListener> tkmsEventsListeners;
  private RateLimiter errorLogRateLimiter = RateLimiter.create(2);

  @PostConstruct
  public void init() {
    for (String topic : properties.getTopics()) {
      validateTopic(properties.getDefaultShard(), topic);
    }
  }

  @Override
  public SendMessagesResult sendMessages(SendMessagesRequest request) {
    validateInput(request);

    Set<String> validatedTopics = new HashSet<>();

    int seq = 0;

    SendMessageResult[] responses = new SendMessageResult[request.getTkmsMessages().size()];

    Map<ShardPartition, List<TkmsMessageWithSequence>> shardPartitionsMap = new HashMap<>();

    for (TkmsMessage tkmsMessage : request.getTkmsMessages()) {
      ShardPartition shardPartition = getShardPartition(tkmsMessage);

      String topic = tkmsMessage.getTopic();
      if (!validatedTopics.contains(topic)) {
        validateTopic(shardPartition.getShard(), topic);
        validatedTopics.add(topic);
      }

      shardPartitionsMap.computeIfAbsent(shardPartition, k -> new ArrayList<>())
          .add(new TkmsMessageWithSequence().setSequence(seq++).setTkmsMessage(tkmsMessage));
    }

    shardPartitionsMap.forEach((shardPartition, tkmsMessageWithSequences) -> {
      List<InsertMessageResult> insertMessageResults = tkmsDao.insertMessages(shardPartition, tkmsMessageWithSequences);
      for (InsertMessageResult insertMessageResult : insertMessageResults) {
        responses[insertMessageResult.getSequence()] =
            new SendMessageResult().setStorageId(insertMessageResult.getStorageId()).setShardPartition(shardPartition);
      }
    });

    return new SendMessagesResult().setResults(Arrays.asList(responses));
  }

  @Override
  public SendMessageResult sendMessage(TkmsMessage message) {
    validateMessage(message);

    ShardPartition shardPartition = getShardPartition(message);

    String topic = message.getTopic();
    validateTopic(shardPartition.getShard(), topic);

    InsertMessageResult insertMessageResult = null;
    try {
      insertMessageResult = tkmsDao.insertMessage(shardPartition, message);
      fireMessageRegisteredEvent(insertMessageResult.getStorageId(), message);
      return new SendMessageResult().setStorageId(insertMessageResult.getStorageId()).setShardPartition(shardPartition);
    } finally {
      if (insertMessageResult == null) {
        metricsTemplate.recordMessageRegistering(topic, null, false);
      } else {
        metricsTemplate.recordMessageRegistering(topic, insertMessageResult.getShardPartition(), true);
      }
    }
  }

  /**
   * Every call to normal KafkaProducer.send() uses metadata for a topic as well, so should be very fast.
   */
  protected void validateTopic(int shard, String topic) {
    kafkaProducerProvider.getKafkaProducer(shard).partitionsFor(topic);
  }

  /**
   * Can not trust the @Valid annotation.
   */
  protected void validateMessage(TkmsMessage message) {
    Set<ConstraintViolation<TkmsMessage>> violations = validator.validate(message);
    if (!violations.isEmpty()) {
      throw new ConstraintViolationException(violations);
    }

    if (message.getShard() != null && message.getShard() >= properties.getShardsCount()) {
      throw new IllegalArgumentException("Shard " + message.getShard() + " is out of bounds. Shards count is " + properties.getShardsCount());
    }
  }

  protected <T> void validateInput(T input) {
    Set<ConstraintViolation<T>> violations = validator.validate(input);
    if (!violations.isEmpty()) {
      throw new ConstraintViolationException(violations);
    }
  }

  protected void fireMessageRegisteredEvent(Long id, TkmsMessage message) {
    List<ITkmsEventsListener> listeners = getTkmsEventsListeners();
    if (tkmsEventsListeners.isEmpty()) {
      return;
    }

    MessageRegisteredEvent event = new MessageRegisteredEvent().setId(id).setMessage(message);

    listeners.forEach(listener -> {
      try {
        listener.messageRegistered(event);
      } catch (Throwable t) {
        if (errorLogRateLimiter.tryAcquire()) {
          log.error(t.getMessage(), t);
        }
      }
    });
  }

  // Lazy to avoid any circular dependencies from low-quality apps.
  protected List<ITkmsEventsListener> getTkmsEventsListeners() {
    if (tkmsEventsListeners == null) {
      synchronized (this) {
        if (tkmsEventsListeners == null) {
          tkmsEventsListeners = new ArrayList<>(applicationContext.getBeansOfType(ITkmsEventsListener.class).values());
        }
      }
    }
    return tkmsEventsListeners;
  }

  protected ShardPartition getShardPartition(TkmsMessage message) {
    int shard = properties.getDefaultShard();
    if (message.getShard() != null) {
      shard = message.getShard();
    }
    if (shard >= properties.getShardsCount()) {
      throw new IllegalArgumentException("Given shard " + message.getShard() + " is out of bounds.");
    }

    int partition = getPartition(shard, message);

    return ShardPartition.of(shard, partition);
  }

  protected int getPartition(int shard, TkmsMessage message) {
    int tablesCount = properties.getPartitionsCount(shard);
    if (tablesCount == 1) {
      return 0;
    }
    if (message.getPartition() != null) {
      return Math.abs(message.getPartition()) % tablesCount;
    }
    if (message.getKey() != null) {
      return Math.abs(message.getKey().hashCode() % tablesCount);
    }
    return ThreadLocalRandom.current().nextInt(tablesCount);
  }

}
