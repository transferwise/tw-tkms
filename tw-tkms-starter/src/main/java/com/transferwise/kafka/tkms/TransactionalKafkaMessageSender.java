package com.transferwise.kafka.tkms;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener.MessageRegisteredEvent;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsMessage.Header;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsDaoProvider;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.config.TkmsProperties.DatabaseDialect;
import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationLevel;
import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationType;
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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Slf4j
public class TransactionalKafkaMessageSender implements ITransactionalKafkaMessageSender, InitializingBean {

  private static final int FIELD_SIZE_BYTES = 6;

  @Autowired
  private ITkmsDaoProvider tkmsDaoProvider;
  @Autowired
  private ITkmsMetricsTemplate metricsTemplate;
  @Autowired
  private TkmsProperties properties;
  @Autowired
  private ApplicationContext applicationContext;
  @Autowired
  private ITkmsKafkaProducerProvider kafkaProducerProvider;
  @Autowired
  private IEnvironmentValidator environmentValidator;
  @Autowired
  private ITransactionsHelper transactionsHelper;
  @Autowired
  private IProblemNotifier problemNotifier;

  private volatile List<ITkmsEventsListener> tkmsEventsListeners;
  private RateLimiter errorLogRateLimiter = RateLimiter.create(2);

  @Override
  public void afterPropertiesSet() {
    Assertions.setLevel(properties.getInternals().getAssertionLevel());

    environmentValidator.validate();

    for (String topic : properties.getTopics()) {
      validateTopic(properties.getDefaultShard(), topic);
    }

    validateDeleteBatchSizes();

    validateEarliestVisibleMessages();
  }

  protected void validateEarliestVisibleMessages() {
    for (int s = 0; s < properties.getShardsCount(); s++) {
      if (properties.getDatabaseDialect(s) == DatabaseDialect.POSTGRES) {
        if (!properties.getEarliestVisibleMessages(s).isEnabled()) {
          int shard = s;
          problemNotifier.notify(s, NotificationType.EARLIEST_MESSAGES_SYSTEM_DISABLED, NotificationLevel.ERROR, () ->
              "Earliest messages system is not enabled for a Postgres database on shard " + shard + ". This can create a serious"
                  + " performance issue when autovacuum gets behind."
          );
        }
      }
    }
  }

  protected void validateDeleteBatchSizes() {
    for (int s = 0; s < properties.getShardsCount(); s++) {
      validateDeleteBatchSize(s, properties.getDeleteBatchSizes(s), "shard " + s);
    }
  }

  protected void validateDeleteBatchSize(int shard, List<Integer> batchSizes, String subject) {
    if (batchSizes == null || batchSizes.isEmpty()) {
      throw new IllegalStateException("Invalid delete batch sizes provided for '" + subject + "', no batches provided.");
    }

    if (batchSizes.get(batchSizes.size() - 1) != 1) {
      throw new IllegalStateException("Invalid delete batch sizes provided for '" + subject + "', last element has to be 1.");
    }

    if (batchSizes.size() > 10) {
      problemNotifier.notify(shard, NotificationType.TOO_MANY_DELETE_BATCHES, NotificationLevel.WARN,
          () -> "Too many delete batches (" + batchSizes.size() + ") can create metrics with too high cardinality.");
    }

    for (int i = 0; i < batchSizes.size(); i++) {
      int size = batchSizes.get(i);

      if (size < 1) {
        throw new IllegalStateException("Invalid delete batch sizes provided for '" + subject + "', " + size + "<1.");
      }

      if (i > 0) {
        int prevSize = batchSizes.get(i - 1);
        if (prevSize <= size) {
          throw new IllegalStateException("Invalid delete batch sizes provided for '" + subject + "', " + prevSize + "<=" + size + ".");
        }
      }
    }
  }

  protected void checkActiveTransaction(List<TkmsMessage> tkmsMessages) {
    boolean transactionActive = TransactionSynchronizationManager.isActualTransactionActive();
    for (int i = 0; i < tkmsMessages.size(); i++) {
      var tkmsMessage = tkmsMessages.get(i);
      checkActiveTransaction(tkmsMessage, transactionActive);
    }
  }

  protected void checkActiveTransaction(TkmsMessage tkmsMessage) {
    checkActiveTransaction(tkmsMessage, TransactionSynchronizationManager.isActualTransactionActive());
  }

  protected void checkActiveTransaction(TkmsMessage tkmsMessage, boolean transactionActive) {
    if (!transactionActive) {
      problemNotifier.notify(tkmsMessage.getShard(), NotificationType.NO_ACTIVE_TRANSACTION, NotificationLevel.BLOCK,
          () -> "No active transaction detected. TKMS is an implementation for the transactional outbox pattern. It is more"
              + " efficient to send direct Kafka messages, when you do not need that pattern.");
    }
  }

  @Override
  public SendMessagesResult sendMessages(SendMessagesRequest request) {
    checkActiveTransaction(request.getTkmsMessages());

    return transactionsHelper.withTransaction().call(() -> {
      validateMessages(request);

      Set<String> validatedTopics = new HashSet<>();

      int seq = 0;

      List<TkmsMessage> tkmsMessages = request.getTkmsMessages();
      int tmksMessagesCount = tkmsMessages.size();
      SendMessageResult[] responses = new SendMessageResult[tmksMessagesCount];

      Map<TkmsShardPartition, List<TkmsMessageWithSequence>> shardPartitionsMap = new HashMap<>();

      for (int messageIdx = 0; messageIdx < tmksMessagesCount; messageIdx++) {
        TkmsMessage tkmsMessage = tkmsMessages.get(messageIdx);
        validateMessageSize(tkmsMessage, messageIdx);

        TkmsShardPartition shardPartition = getShardPartition(tkmsMessage);

        String topic = tkmsMessage.getTopic();
        if (!validatedTopics.contains(topic)) {
          validateTopic(shardPartition.getShard(), topic);
          validatedTopics.add(topic);
        }

        TkmsMessageWithSequence tkmsMessageWithSequence = new TkmsMessageWithSequence().setSequence(seq++).setTkmsMessage(tkmsMessage);
        shardPartitionsMap.computeIfAbsent(shardPartition, k -> new ArrayList<>()).add(tkmsMessageWithSequence);
      }

      shardPartitionsMap.forEach((shardPartition, tkmsMessageWithSequences) -> {
        try {
          shardPartition.putIntoMdc();

          var tkmsDao = tkmsDaoProvider.getTkmsDao(shardPartition.getShard());

          List<InsertMessageResult> insertMessageResults = tkmsDao.insertMessages(shardPartition, tkmsMessageWithSequences);
          for (int i = 0; i < tkmsMessageWithSequences.size(); i++) {
            TkmsMessageWithSequence tkmsMessageWithSequence = tkmsMessageWithSequences.get(i);
            InsertMessageResult insertMessageResult = insertMessageResults.get(i);

            fireMessageRegisteredEvent(shardPartition, insertMessageResult.getStorageId(), tkmsMessageWithSequence.getTkmsMessage());

            metricsTemplate.recordMessageRegistering(tkmsMessageWithSequence.getTkmsMessage().getTopic(), shardPartition);

            responses[insertMessageResult.getSequence()] =
                new SendMessageResult().setStorageId(insertMessageResult.getStorageId()).setShardPartition(shardPartition);
          }
        } finally {
          shardPartition.removeFromMdc();
        }
      });

      return new SendMessagesResult().setResults(Arrays.asList(responses));
    });
  }

  @Override
  public SendMessageResult sendMessage(TkmsMessage message) {
    var shardPartition = getShardPartition(message);

    try {
      shardPartition.putIntoMdc();

      checkActiveTransaction(message);

      return transactionsHelper.withTransaction().call(() -> {
        validateMessage(message, 0);
        validateMessageSize(message, 0);

        String topic = message.getTopic();
        validateTopic(shardPartition.getShard(), topic);

        var tkmsDao = tkmsDaoProvider.getTkmsDao(shardPartition.getShard());
        InsertMessageResult insertMessageResult = tkmsDao.insertMessage(shardPartition, message);
        fireMessageRegisteredEvent(shardPartition, insertMessageResult.getStorageId(), message);
        metricsTemplate.recordMessageRegistering(topic, insertMessageResult.getShardPartition());
        return new SendMessageResult().setStorageId(insertMessageResult.getStorageId()).setShardPartition(shardPartition);
      });
    } finally {
      shardPartition.removeFromMdc();
    }
  }

  /**
   * Every call to normal `KafkaProducer.send()` uses metadata for a topic as well, so should be very fast.
   */
  protected void validateTopic(int shard, String topic) {
    kafkaProducerProvider.getKafkaProducer(shard).partitionsFor(topic);
  }

  protected void validateMessages(SendMessagesRequest request) {
    for (int i = 0; i < request.getTkmsMessages().size(); i++) {
      TkmsMessage tkmsMessage = request.getTkmsMessages().get(i);
      validateMessage(tkmsMessage, i);
    }
  }

  protected void validateMessage(TkmsMessage message, int messageIdx) {
    Preconditions.checkNotNull(message, "%s: No message provided.", messageIdx);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(message.getTopic()), "%s: No topic provided.", messageIdx);
    if (message.getPartition() != null) {
      Preconditions.checkArgument(message.getPartition() >= 0, "%s: Partition number can not be negative: %s", messageIdx, message.getPartition());
    }
    if (message.getKey() != null) {
      Preconditions.checkArgument(message.getKey().length() > 0, "%s: Key can not be an empty string.", messageIdx);
    }
    if (message.getShard() != null) {
      Preconditions.checkArgument(message.getShard() >= 0, "%s: Shard number can not be negative :%s", messageIdx, message.getShard());
      Preconditions.checkArgument(message.getShard() < properties.getShardsCount(), "%s: Shard %s is out of bounds. Shards count is %s.", messageIdx,
          message.getShard(), properties.getShardsCount());
    }
    Preconditions.checkNotNull(message.getValue(), "%s: Value can not be null.", messageIdx);
    if (message.getHeaders() != null) {
      for (int headerIdx = 0; headerIdx < message.getHeaders().size(); headerIdx++) {
        Header header = message.getHeaders().get(headerIdx);
        Preconditions.checkNotNull(header.getValue(), "%s: Header value @{%s} can not be null.", messageIdx, headerIdx);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(header.getKey()), "%s: Header key @{%s} can not be null.", messageIdx, headerIdx);
      }
    }
  }

  /**
   * The initial size and padding will use conservative values of 100 and 6.
   *
   * <p>This would allow to calculate the size safely while not tying our code to some internal kafka client methods.
   */
  protected void validateMessageSize(TkmsMessage message, int messageIdx) {
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
          "" + messageIdx + ": Estimated message size is " + size + ", which is larger than maximum of " + properties.getMaximumMessageBytes() + ".");
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
    if (log.isDebugEnabled()) {
      log.debug("Message was registered for " + shardPartition + " with storage id " + id + ". Listeners count: " + listeners.size());
    }

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
