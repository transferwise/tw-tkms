package com.transferwise.kafka.tkms;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.TransactionContext.Mode;
import com.transferwise.kafka.tkms.TransactionContext.ShardPartitionMessages;
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
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
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

  @Autowired
  @Lazy
  private List<ITkmsEventsListener> tkmsEventsListeners;

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
    for (var s = 0; s < properties.getShardsCount(); s++) {
      if (properties.getDatabaseDialect(s) == DatabaseDialect.POSTGRES) {
        if (!properties.getEarliestVisibleMessages(s).isEnabled()) {
          var shard = s;
          problemNotifier.notify(s, NotificationType.EARLIEST_MESSAGES_SYSTEM_DISABLED, NotificationLevel.ERROR, () ->
              "Earliest messages system is not enabled for a Postgres database on shard " + shard + ". This can create a serious"
                  + " performance issue when autovacuum gets behind."
          );
        }
      }
    }
  }

  protected void validateDeleteBatchSizes() {
    for (var s = 0; s < properties.getShardsCount(); s++) {
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
      var size = batchSizes.get(i);

      if (size < 1) {
        throw new IllegalStateException("Invalid delete batch sizes provided for '" + subject + "', " + size + "<1.");
      }

      if (i > 0) {
        var prevSize = batchSizes.get(i - 1);
        if (prevSize <= size) {
          throw new IllegalStateException("Invalid delete batch sizes provided for '" + subject + "', " + prevSize + "<=" + size + ".");
        }
      }
    }
  }

  protected void checkActiveTransaction(int shard, boolean transactionActive, boolean deferMessageRegistrationUntilCommit) {
    if (!transactionActive) {
      if (deferMessageRegistrationUntilCommit) {
        // We have to block here for sure.
        throw new IllegalStateException("No active transaction detected. It is required when defer-until-commit registration method is requested.");
      } else {
        problemNotifier.notify(shard, NotificationType.NO_ACTIVE_TRANSACTION, NotificationLevel.BLOCK,
            () -> "No active transaction detected. TKMS is an implementation for the transactional outbox pattern. It is more"
                + " efficient to send direct Kafka messages, when you do not need that pattern.");
      }
    }
  }

  @Override
  public SendMessagesResult sendMessages(SendMessagesRequest request) {
    var transactionActive = TransactionSynchronizationManager.isActualTransactionActive();

    validateMessages(request);

    return transactionsHelper.withTransaction().call(() -> {
      var transactionContext = getAndBindTransactionContext();

      var validatedTopics = new HashSet<String>();

      int seq = 0;

      var tkmsMessages = request.getTkmsMessages();
      var tmksMessagesCount = tkmsMessages.size();
      var responses = new SendMessageResult[tmksMessagesCount];

      var shardPartitionsMap = new HashMap<TkmsShardPartition, List<TkmsMessageWithSequence>>();

      for (int messageIdx = 0; messageIdx < tmksMessagesCount; messageIdx++) {
        var tkmsMessage = tkmsMessages.get(messageIdx);
        validateMessageSize(tkmsMessage, messageIdx);

        var shardPartition = getShardPartition(tkmsMessage);

        var topic = tkmsMessage.getTopic();
        if (!validatedTopics.contains(topic)) {
          validateTopic(shardPartition.getShard(), topic);
          validatedTopics.add(topic);
        }

        var tkmsMessageWithSequence = new TkmsMessageWithSequence().setSequence(seq++).setTkmsMessage(tkmsMessage);
        shardPartitionsMap.computeIfAbsent(shardPartition, k -> new ArrayList<>()).add(tkmsMessageWithSequence);
      }

      shardPartitionsMap.forEach((shardPartition, tkmsMessageWithSequences) -> {
        try {
          shardPartition.putIntoMdc();

          var deferMessageRegistrationUntilCommit = properties.deferMessageRegistrationUntilCommit(shardPartition.getShard());
          if (request.getDeferMessageRegistrationUntilCommit() != null) {
            deferMessageRegistrationUntilCommit = request.getDeferMessageRegistrationUntilCommit();
          }

          checkActiveTransaction(shardPartition.getShard(), transactionActive, deferMessageRegistrationUntilCommit);

          if (deferMessageRegistrationUntilCommit) {
            var shardPartitionMessages = transactionContext.getShardPartitionMessages(shardPartition);
            requireConsistentMode(shardPartitionMessages, Mode.DEFERRED);

            for (var i = 0; i < tkmsMessageWithSequences.size(); i++) {
              var message = tkmsMessageWithSequences.get(i);
              shardPartitionMessages.getMessages().add(message.getTkmsMessage());

              // Storage id remains null
              responses[message.getSequence()] = new SendMessageResult().setShardPartition(shardPartition);

              transactionContext.countMessage();
            }
          } else {
            if (transactionActive) {
              var shardPartitionMessages = transactionContext.getShardPartitionMessages(shardPartition);
              requireConsistentMode(shardPartitionMessages, Mode.NOT_DEFERRED);
            }

            var tkmsDao = tkmsDaoProvider.getTkmsDao(shardPartition.getShard());

            var insertMessageResults = tkmsDao.insertMessages(shardPartition, tkmsMessageWithSequences);
            for (var i = 0; i < tkmsMessageWithSequences.size(); i++) {
              var tkmsMessageWithSequence = tkmsMessageWithSequences.get(i);
              var insertMessageResult = insertMessageResults.get(i);

              fireMessageRegisteredEvent(shardPartition, insertMessageResult.getStorageId(), tkmsMessageWithSequence.getTkmsMessage());

              metricsTemplate.recordMessageRegistering(tkmsMessageWithSequence.getTkmsMessage().getTopic(), shardPartition, false);

              responses[insertMessageResult.getSequence()] =
                  new SendMessageResult().setStorageId(insertMessageResult.getStorageId()).setShardPartition(shardPartition);

              transactionContext.countMessage();
            }
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
    return sendMessage(new SendMessageRequest().setTkmsMessage(message));
  }

  @Override
  public SendMessageResult sendMessage(SendMessageRequest request) {
    var transactionActive = TransactionSynchronizationManager.isActualTransactionActive();

    var message = request.getTkmsMessage();
    var shardPartition = getShardPartition(message);

    try {
      shardPartition.putIntoMdc();

      var deferMessageRegistrationUntilCommit = properties.deferMessageRegistrationUntilCommit(shardPartition.getShard());
      if (request.getDeferMessageRegistrationUntilCommit() != null) {
        deferMessageRegistrationUntilCommit = request.getDeferMessageRegistrationUntilCommit();
      }

      checkActiveTransaction(shardPartition.getShard(), transactionActive, deferMessageRegistrationUntilCommit);

      validateMessage(message, 0);
      validateMessageSize(message, 0);

      var topic = message.getTopic();
      validateTopic(shardPartition.getShard(), topic);

      if (deferMessageRegistrationUntilCommit) {
        // Transaction is guaranteed to be active here.
        var transactionContext = getAndBindTransactionContext();
        var shardPartitionMessages = transactionContext.getShardPartitionMessages(shardPartition);
        shardPartitionMessages.getMessages().add(message);
        requireConsistentMode(shardPartitionMessages, Mode.DEFERRED);

        transactionContext.countMessage();
        // Storage id is not known yet.
        return new SendMessageResult().setShardPartition(shardPartition);
      } else {
        return transactionsHelper.withTransaction().call(() -> {
          var transactionContext = getAndBindTransactionContext();

          if (transactionActive) {
            var shardPartitionMessages = transactionContext.getShardPartitionMessages(shardPartition);
            requireConsistentMode(shardPartitionMessages, Mode.NOT_DEFERRED);
          }

          var tkmsDao = tkmsDaoProvider.getTkmsDao(shardPartition.getShard());
          var insertMessageResult = tkmsDao.insertMessage(shardPartition, message);
          fireMessageRegisteredEvent(shardPartition, insertMessageResult.getStorageId(), message);
          metricsTemplate.recordMessageRegistering(topic, insertMessageResult.getShardPartition(), false);
          transactionContext.countMessage();
          return new SendMessageResult().setStorageId(insertMessageResult.getStorageId()).setShardPartition(shardPartition);
        });
      }
    } finally {
      shardPartition.removeFromMdc();
    }
  }

  protected void requireConsistentMode(ShardPartitionMessages shardPartitionMessages, Mode mode) {
    if (shardPartitionMessages.getMode() == null) {
      shardPartitionMessages.setMode(mode);
    } else if (shardPartitionMessages.getMode() != mode) {
      throw new IllegalStateException(
          "You can not mix deferred and not deferred messages in the same transaction, as it will break the ordering guarantees.");
    }
  }

  protected TransactionContext getAndBindTransactionContext() {
    var transactionContext = TransactionContext.get();

    if (transactionContext == null) {
      transactionContext = TransactionContext.createAndBind();
      TransactionSynchronizationManager.registerSynchronization(new BeforeCommitSender());
    }

    return transactionContext;
  }

  // Extending deprecated class, so it would also work on Spring 4.
  private class BeforeCommitSender extends TransactionSynchronizationAdapter {

    @Override
    public void beforeCommit(boolean readOnly) {
      var transactionContext = TransactionContext.get();

      for (var entries : transactionContext.getShardPartitionMessagesMap().entrySet()) {
        var shardPartition = entries.getKey();
        var messages = entries.getValue().getMessages();

        var tkmsDao = tkmsDaoProvider.getTkmsDao(shardPartition.getShard());

        var messagesWithSequences = new ArrayList<TkmsMessageWithSequence>();
        for (int i = 0; i < messages.size(); i++) {
          messagesWithSequences.add(new TkmsMessageWithSequence().setSequence(i).setTkmsMessage(messages.get(i)));
        }

        var insertMessageResults = tkmsDao.insertMessages(shardPartition, messagesWithSequences);
        for (int i = 0; i < messagesWithSequences.size(); i++) {
          var tkmsMessageWithSequence = messagesWithSequences.get(i);
          var insertMessageResult = insertMessageResults.get(i);

          fireMessageRegisteredEvent(shardPartition, insertMessageResult.getStorageId(), tkmsMessageWithSequence.getTkmsMessage());

          metricsTemplate.recordMessageRegistering(tkmsMessageWithSequence.getTkmsMessage().getTopic(), shardPartition, true);
        }
      }
    }

    @Override
    public void afterCompletion(int status) {
      boolean success = TransactionSynchronization.STATUS_COMMITTED == status;
      var transactionContext = getAndBindTransactionContext();

      metricsTemplate.registerMessagesInTransactionCount(transactionContext.getRegisteredMessagesCount(), success);

      TransactionContext.unbind();
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
      var tkmsMessage = request.getTkmsMessages().get(i);
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
    var count = 0;
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
    var listeners = getTkmsEventsListeners();
    if (log.isDebugEnabled()) {
      log.debug("Message was registered for {} with storage id {}. Listeners count: ", shardPartition, id, listeners.size());
    }

    if (tkmsEventsListeners.isEmpty()) {
      return;
    }

    var event = new MessageRegisteredEvent().setStorageId(id).setMessage(message).setShardPartition(shardPartition);

    listeners.forEach(listener -> {
      try {
        listener.messageRegistered(event);
      } catch (Throwable t) {
        if (errorLogRateLimiter.tryAcquire()) {
          log.error("Firing message registered event failed for listener '{}'.", listener, t);
        }
      }
    });
  }

  // Allow to override on older Spring services not support Lazy annotation properly.
  protected List<ITkmsEventsListener> getTkmsEventsListeners() {
    return tkmsEventsListeners;
  }

  protected TkmsShardPartition getShardPartition(TkmsMessage message) {
    var shard = properties.getDefaultShard();
    if (message.getShard() != null) {
      shard = message.getShard();
    }
    if (shard >= properties.getShardsCount()) {
      throw new IllegalArgumentException("Given shard " + message.getShard() + " is out of bounds.");
    }

    var partition = getPartition(shard, message);

    return TkmsShardPartition.of(shard, partition);
  }

  protected int getPartition(int shard, TkmsMessage message) {
    var tablesCount = properties.getPartitionsCount(shard);
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
