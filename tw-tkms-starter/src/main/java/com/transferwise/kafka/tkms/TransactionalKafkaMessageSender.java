package com.transferwise.kafka.tkms;

import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener.MessageRegisteredEvent;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.Message;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.dao.ITkmsDao.InsertMessageResult;
import com.transferwise.kafka.tkms.metrics.IMetricsTemplate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

  private volatile List<ITkmsEventsListener> tkmsEventsListeners;

  private RateLimiter errorLogRateLimiter = RateLimiter.create(2);

  @Override
  @Transactional
  public long sendMessage(Message message) {
    validateMessage(message);

    InsertMessageResult insertMessageResult = null;
    try {
      insertMessageResult = transactionalKafkaMessageSenderDao.insertMessage(message);
      fireMessageRegisteredEvent(insertMessageResult.getId(), message);
      return insertMessageResult.getId();
    } finally {
      if (insertMessageResult == null) {
        metricsTemplate.recordMessageRegistering(message.getTopic(), null, false);
      } else {
        metricsTemplate.recordMessageRegistering(message.getTopic(), insertMessageResult.getShardPartition(), true);
      }
    }
  }

  /**
   * Can not trust the @Valid annotation.
   */
  protected void validateMessage(Message message) {
    Set<ConstraintViolation<Message>> violations = validator.validate(message);
    if (!violations.isEmpty()) {
      throw new ConstraintViolationException(violations);
    }

    if (message.getShard() != null && message.getShard() >= properties.getShardsCount()) {
      throw new IllegalArgumentException("Shard " + message.getShard() + " is out of bounds. Shards count is " + properties.getShardsCount());
    }
  }

  protected void fireMessageRegisteredEvent(Long id, Message message) {
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

}
