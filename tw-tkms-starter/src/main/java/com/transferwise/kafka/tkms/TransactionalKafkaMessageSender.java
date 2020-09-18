package com.transferwise.kafka.tkms;

import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener.MessageRegisteredEvent;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsMessage.Header;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.dao.ITkmsDao.InsertMessageResult;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
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

  private static int FIELD_SIZE_BYTES = 6;

  @Autowired
  private ITkmsDao tkmsDao;
  @Autowired
  private Validator validator;
  @Autowired
  private ITkmsMetricsTemplate metricsTemplate;
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
    metricsTemplate.registerLibrary();
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

    Map<TkmsShardPartition, List<TkmsMessageWithSequence>> shardPartitionsMap = new HashMap<>();

    for (TkmsMessage tkmsMessage : request.getTkmsMessages()) {
      validateMessageSize(tkmsMessage);

      TkmsShardPartition shardPartition = getShardPartition(tkmsMessage);

      String topic = tkmsMessage.getTopic();
      if (!validatedTopics.contains(topic)) {
        validateTopic(shardPartition.getShard(), topic);
        validatedTopics.add(topic);
      }

      TkmsMessageWithSequence tkmsMessageWithSequence = new TkmsMessageWithSequence().setSequence(seq++).setTkmsMessage(tkmsMessage);
      shardPartitionsMap.computeIfAbsent(shardPartition, k -> new ArrayList<>())
          .add(tkmsMessageWithSequence);
    }

    shardPartitionsMap.forEach((shardPartition, tkmsMessageWithSequences) -> {
      List<InsertMessageResult> insertMessageResults = tkmsDao.insertMessages(shardPartition, tkmsMessageWithSequences);
      for (int i = 0; i < tkmsMessageWithSequences.size(); i++) {
        TkmsMessageWithSequence tkmsMessageWithSequence = tkmsMessageWithSequences.get(i);
        InsertMessageResult insertMessageResult = insertMessageResults.get(i);

        fireMessageRegisteredEvent(shardPartition, insertMessageResult.getStorageId(), tkmsMessageWithSequence.getTkmsMessage());

        metricsTemplate.recordMessageRegistering(tkmsMessageWithSequence.getTkmsMessage().getTopic(), shardPartition);

        responses[insertMessageResult.getSequence()] =
            new SendMessageResult().setStorageId(insertMessageResult.getStorageId()).setShardPartition(shardPartition);
      }
    });

    return new SendMessagesResult().setResults(Arrays.asList(responses));
  }

  @Override
  public SendMessageResult sendMessage(TkmsMessage message) {
    validateMessage(message);
    validateMessageSize(message);

    TkmsShardPartition shardPartition = getShardPartition(message);

    String topic = message.getTopic();
    validateTopic(shardPartition.getShard(), topic);

    InsertMessageResult insertMessageResult = null;
    insertMessageResult = tkmsDao.insertMessage(shardPartition, message);
    fireMessageRegisteredEvent(shardPartition, insertMessageResult.getStorageId(), message);
    metricsTemplate.recordMessageRegistering(topic, insertMessageResult.getShardPartition());
    return new SendMessageResult().setStorageId(insertMessageResult.getStorageId()).setShardPartition(shardPartition);
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

  /**
   * The initial size and padding will use conservative values of 100 and 6.
   *
   * <p>This would allow to calculate the size safely while not tying our code to some internal kafka client methods.
   */
  protected void validateMessageSize(TkmsMessage message) {
    int size = 100;

    size += FIELD_SIZE_BYTES;
    size += utf8Length(message.getKey());
    size += FIELD_SIZE_BYTES;
    size += message.getValue().length;
    size += FIELD_SIZE_BYTES;

    if (message.getHeaders() != null) {
      for (Header header : message.getHeaders()) {
        size += FIELD_SIZE_BYTES;
        size += utf8Length(header.getKey());
        size += FIELD_SIZE_BYTES;
        size += header.getValue() == null ? 0 : header.getValue().length;
      }
    }

    if (size >= properties.getMaximumMessageBytes()) {
      throw new IllegalArgumentException(
          "Estimated message size is " + size + ", which is larger than maximum of " + properties.getMaximumMessageBytes() + ".");
    }
  }

  private int utf8Length(CharSequence s) {
    if (s == null) {
      return 0;
    }
    int count = 0;
    for (int i = 0, len = s.length(); i < len; i++) {
      char ch = s.charAt(i);
      if (ch <= 0x7F) {
        count++;
      } else if (ch <= 0x7FF) {
        count += 2;
      } else if (Character.isHighSurrogate(ch)) {
        count += 4;
        ++i;
      } else {
        count += 3;
      }
    }
    return count;
  }

  protected void fireMessageRegisteredEvent(TkmsShardPartition shardPartition, Long id, TkmsMessage message) {
    List<ITkmsEventsListener> listeners = getTkmsEventsListeners();
    log.debug("Message was registered for " + shardPartition + " with storage id " + id + ". Listeners count: " + listeners.size());

    if (tkmsEventsListeners.isEmpty()) {
      return;
    }

    MessageRegisteredEvent event = new MessageRegisteredEvent().setStorageId(id).setMessage(message).setShardPartition(shardPartition);

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

  protected TkmsShardPartition getShardPartition(TkmsMessage message) {
    int shard = properties.getDefaultShard();
    if (message.getShard() != null) {
      shard = message.getShard();
    }
    if (shard >= properties.getShardsCount()) {
      throw new IllegalArgumentException("Given shard " + message.getShard() + " is out of bounds.");
    }

    int partition = getPartition(shard, message);

    return TkmsShardPartition.of(shard, partition);
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
