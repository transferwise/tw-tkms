package com.transferwise.kafka.tkms;

import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener.MessageRegisteredEvent;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.dao.ITkmsDao.InsertMessageResult;
import com.transferwise.kafka.tkms.metrics.IMetricsTemplate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.PostConstruct;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
public class TransactionalKafkaMessageSender implements ITransactionalKafkaMessageSender {

  @Autowired
  private ITkmsDao transactionalKafkaMessageSenderDao;
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
  @Transactional
  public long sendMessage(TkmsMessage message) {
    validateMessage(message);

    int shard = getShard(message);

    String topic = message.getTopic();
    validateTopic(shard, topic);

    InsertMessageResult insertMessageResult = null;
    try {
      insertMessageResult = transactionalKafkaMessageSenderDao.insertMessage(shard, message);
      fireMessageRegisteredEvent(insertMessageResult.getId(), message);
      return insertMessageResult.getId();
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

  protected int getShard(TkmsMessage message) {
    if (message.getShard() == null) {
      return properties.getDefaultShard();
    }
    if (message.getShard() >= properties.getShardsCount()) {
      throw new IllegalArgumentException("Given shard " + message.getShard() + " is out of bounds.");
    }

    return message.getShard();
  }

}
