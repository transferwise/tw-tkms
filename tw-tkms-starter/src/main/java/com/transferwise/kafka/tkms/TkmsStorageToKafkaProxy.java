package com.transferwise.kafka.tkms;

import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.common.leaderselector.ILock;
import com.transferwise.common.leaderselector.Leader.Control;
import com.transferwise.common.leaderselector.LeaderSelectorV2;
import com.transferwise.common.leaderselector.SharedReentrantLockBuilderFactory;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener.MessageAcknowledgedEvent;
import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptor.MessageInterceptionDecision;
import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptors;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.dao.ITkmsDao.MessageRecord;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import io.micrometer.core.instrument.Gauge;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import lombok.Data;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

@Slf4j
public class TkmsStorageToKafkaProxy implements GracefulShutdownStrategy, ITkmsStorageToKafkaProxy {

  @Autowired
  private ITkmsKafkaProducerProvider tkmsKafkaProducerProvider;
  @Autowired
  private IExecutorServicesProvider executorServicesProvider;
  @Autowired
  private TkmsProperties properties;
  @Autowired
  @Setter
  private ITkmsDao dao;
  @Autowired
  private ITkmsPaceMaker tkmsPaceMaker;
  @Autowired
  private ITkmsZookeeperOperations zkOperations;
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;
  @Autowired
  private ITkmsMetricsTemplate metricsTemplate;
  @Autowired
  private ApplicationContext applicationContext;
  @Autowired
  private ITkmsMessageInterceptors messageIntereceptors;
  @Autowired
  private SharedReentrantLockBuilderFactory lockBuilderFactory;

  @TestOnly
  private volatile boolean paused = false;
  @TestOnly
  private volatile boolean pauseRequested = false;

  private volatile List<ITkmsEventsListener> tkmsEventsListeners;
  private final List<LeaderSelectorV2> leaderSelectors = new ArrayList<>();
  private RateLimiter exceptionRateLimiter = RateLimiter.create(2);


  @PostConstruct
  public void init() {
    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(s); p++) {
        TkmsShardPartition shardPartition = TkmsShardPartition.of(s, p);

        ExecutorService executorService =
            new ThreadNamingExecutorServiceWrapper("tw-tkms-poller-" + s + "_" + p, executorServicesProvider.getGlobalExecutorService());

        ILock lock = lockBuilderFactory.createBuilder(zkOperations.getLockNodePath(shardPartition)).build();

        leaderSelectors.add(new LeaderSelectorV2.Builder().setLock(lock).setExecutorService(executorService).setLeader(control -> {
          AtomicReference<Future<Boolean>> futureReference = new AtomicReference<>();
          AtomicReference<Object> pollingGauge = new AtomicReference<>();
          
          control.workAsyncUntilShouldStop(() -> futureReference.set(executorService.submit(
                  () -> {
                    try {
                      log.info("Starting to proxy {}.", shardPartition);
                      pollingGauge.set(metricsTemplate.registerPollingGauge(shardPartition));
                      poll(control, shardPartition);
                      return true;
                    } catch (Throwable t) {
                      log.error(t.getMessage(), t);
                      return false;
                    } finally {
                      control.yield();
                    }
                  })),
              () -> {
                log.info("Stopping proxying for {}.", shardPartition);
                
                // TODO: Application with larger amount of shards could benefit of closing unused kafka producers here?

                Future<Boolean> future = futureReference.get();
                if (future != null) {
                  try {
                    Boolean result = future.get(tkmsPaceMaker.getLongWaitTime(shardPartition.getShard()).toMillis(), TimeUnit.MILLISECONDS);
                    if (result == null) {
                      throw new IllegalStateException("Hang detected when trying to stop polling of " + shardPartition + ".");
                    }
                  } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                  }
                }
                
                var gauge = pollingGauge.get();
                if (gauge != null) {
                  metricsTemplate.unregisterMetric(gauge);
                  pollingGauge.set(null);
                }
              });
        }).build());
      }
    }
  }

  private void poll(Control control, TkmsShardPartition shardPartition) {
    int pollerBatchSize = properties.getPollerBatchSize(shardPartition.getShard());
    long startTimeMs = System.currentTimeMillis();

    long timeToLiveMs = properties.getProxyTimeToLive().toMillis() + ThreadLocalRandom.current().nextLong(TimeUnit.SECONDS.toMillis(5));
    final Map<Integer, ProducerRecord<String, byte[]>> producerRecordMap = new HashMap<>();

    EarliestMessageTracker earliestMessageTracker = new EarliestMessageTracker(shardPartition, dao, properties, metricsTemplate);
    earliestMessageTracker.init();

    try {
      MutableObject<Duration> proxyCyclePauseRequest = new MutableObject<>();

      while (!control.shouldStop()) {
        if (pauseRequested) {
          paused = true;
          tkmsPaceMaker.doSmallPause(shardPartition.getShard());
          return;
        }

        if (System.currentTimeMillis() - startTimeMs > timeToLiveMs) {
          // Poor man's load balancer. Allow other nodes a chance to get a leader as well.
          // TODO: investigate how Kafka client does it and replicate.
          if (log.isDebugEnabled()) {
            log.debug("Yielding control for " + shardPartition + ". " + (System.currentTimeMillis() - startTimeMs) + " has passed.");
          }
          return;
        }

        if (proxyCyclePauseRequest.getValue() != null) {
          var pauseTimeMs = proxyCyclePauseRequest.getValue().toMillis();
          if (pauseTimeMs > 0) {
            ExceptionUtils.doUnchecked(() -> Thread.sleep(pauseTimeMs));
            metricsTemplate.recordProxyCyclePause(shardPartition, pauseTimeMs);
          }
          proxyCyclePauseRequest.setValue(null);
        }

        unitOfWorkManager.createEntryPoint("TKMS", "poll_" + shardPartition.getShard() + "_" + shardPartition.getPartition()).toContext()
            .execute(() -> {
              long cycleStartNanoTime = System.nanoTime();
              int polledRecordsCount = 0;
              AtomicInteger failedSendsCount = new AtomicInteger();
              try {
                List<MessageRecord> records = dao.getMessages(shardPartition, earliestMessageTracker.getEarliestMessageId(), pollerBatchSize);
                polledRecordsCount = records.size();

                metricsTemplate.recordProxyPoll(shardPartition, polledRecordsCount, cycleStartNanoTime);
                proxyCyclePauseRequest.setValue(tkmsPaceMaker.getPollingPause(shardPartition, pollerBatchSize, polledRecordsCount));

                if (polledRecordsCount == 0) {
                  return;
                }

                earliestMessageTracker.register(records.get(0).getId());

                MessageProcessingContext[] contexts = new MessageProcessingContext[records.size()];

                final long kafkaSendStartNanoTime = System.nanoTime();
                KafkaProducer<String, byte[]> kafkaProducer = tkmsKafkaProducerProvider.getKafkaProducer(shardPartition.getShard());
                boolean atLeastOneSendDone = false;

                producerRecordMap.clear();
                Map<Integer, MessageInterceptionDecision> interceptionDecisions = null;
                if (messageIntereceptors.hasInterceptors()) {
                  for (int i = 0; i < records.size(); i++) {
                    MessageRecord messageRecord = records.get(i);
                    producerRecordMap.put(i, toProducerRecord(messageRecord));
                  }
                  interceptionDecisions = messageIntereceptors.beforeSendingToKafka(shardPartition, producerRecordMap);
                }

                for (int i = 0; i < records.size(); i++) {
                  MessageRecord messageRecord = records.get(i);
                  ProducerRecord<String, byte[]> preCreatedProducerRecord = producerRecordMap.get(i);
                  ProducerRecord<String, byte[]> producerRecord =
                      preCreatedProducerRecord == null ? toProducerRecord(messageRecord) : preCreatedProducerRecord;
                  contexts[i] = new MessageProcessingContext().setProducerRecord(producerRecord).setMessageRecord(messageRecord)
                      .setShardPartition(shardPartition);
                  MessageProcessingContext context = contexts[i];

                  MessageInterceptionDecision interceptionDecision = interceptionDecisions == null ? null : interceptionDecisions.get(i);
                  if (interceptionDecision != null) {
                    if (interceptionDecision == MessageInterceptionDecision.DISCARD) {
                      log.warn("Discarding message {}:{}.", shardPartition, messageRecord.getId());
                      context.setAcked(true);
                      continue;
                    } else if (interceptionDecision == MessageInterceptionDecision.RETRY) {
                      // In this context retry means - allowing interceptors to try to execute their logic again.
                      continue;
                    }
                  }

                  try {
                    // Theoretically, to be absolutely sure, about the ordering, we would need to wait for the future result immediately.
                    // But it would not be practical. I mean we could send one message from each partitions concurrently, but
                    // there is a high chance that all the messages in this thread would reside in the same transaction, so it would not work.
                    // TODO: Consider transactions. They would need heavy performance testing though.
                    Future<RecordMetadata> future = kafkaProducer.send(producerRecord, (metadata, exception) -> {
                      if (exception == null) {
                        context.setAcked(true);
                        fireMessageAcknowledgedEvent(shardPartition, messageRecord.getId(), producerRecord);
                        Instant insertTime = messageRecord.getMessage().hasInsertTimestamp()
                            ? Instant.ofEpochMilli(messageRecord.getMessage().getInsertTimestamp().getValue()) : null;
                        metricsTemplate.recordProxyMessageSendSuccess(shardPartition, producerRecord.topic(), insertTime);
                      } else {
                        failedSendsCount.incrementAndGet();
                        handleKafkaError(shardPartition, "Sending message " + messageRecord.getId() + " in " + shardPartition + " failed.", exception,
                            context);
                        metricsTemplate.recordProxyMessageSendFailure(shardPartition, producerRecord.topic());
                      }
                    });
                    atLeastOneSendDone = true;

                    contexts[i].setKafkaSenderFuture(future);
                  } catch (Throwable t) {
                    failedSendsCount.incrementAndGet();
                    handleKafkaError(shardPartition, "Sending message " + messageRecord.getId() + " in " + shardPartition + " failed.", t, context);
                  }
                }

                if (atLeastOneSendDone) {
                  kafkaProducer.flush();
                }

                for (int i = 0; i < records.size(); i++) {
                  MessageProcessingContext context = contexts[i];
                  if (context.getKafkaSenderFuture() != null) {
                    try {
                      context.getKafkaSenderFuture().get();
                    } catch (Throwable t) {
                      handleKafkaError(shardPartition, "Sending message in " + shardPartition + " failed.", t, context);
                    }
                  }
                }

                metricsTemplate.recordProxyKafkaMessagesSend(shardPartition, kafkaSendStartNanoTime);

                List<Long> successIds = new ArrayList<>();

                for (int i = 0; i < records.size(); i++) {
                  MessageProcessingContext context = contexts[i];
                  if (context.isAcked()) {
                    successIds.add(records.get(i).getId());
                  }
                }
                //TODO: In current implementation this can create latency (but not reduce total throughput).
                // In the future we may provide more algorithms here.
                //   For example we want to probably offload deleting into a separate thread(s)
                //   Select would need id>X, which probably would not be too bad.
                long deleteStartNanoTime = System.nanoTime();
                dao.deleteMessages(shardPartition, successIds);

                metricsTemplate.recordProxyMessagesDeletion(shardPartition, deleteStartNanoTime);

                if (failedSendsCount.get() > 0) {
                  proxyCyclePauseRequest.setValue(tkmsPaceMaker.getPollingPauseOnError(shardPartition));
                }
              } catch (Throwable t) {
                log.error(t.getMessage(), t);
                proxyCyclePauseRequest.setValue(tkmsPaceMaker.getPollingPauseOnError(shardPartition));
              } finally {
                metricsTemplate.recordProxyCycle(shardPartition, polledRecordsCount, cycleStartNanoTime);
              }
            });
      }
    } finally {
      earliestMessageTracker.shutdown();
    }
  }

  /**
   * The idea is to avoid spam when for example Kafka cluster is upgraded and/or topics are rebalanced.
   *
   * <p>But at the same time it would be quite risky to ignore all RetriableExceptions, so we log at least some.
   */
  protected void handleKafkaError(TkmsShardPartition shardPartition, String message, Throwable t, MessageProcessingContext context) {
    if (t instanceof RetriableException) {
      if (exceptionRateLimiter.tryAcquire()) {
        log.error(message, t);
      }
    } else {
      log.error(message, t);
    }

    MessageInterceptionDecision decision = messageIntereceptors.onError(shardPartition, t, context.getProducerRecord());
    if (decision == MessageInterceptionDecision.DISCARD) {
      log.warn("Discarding message {}:{}.", context.getShardPartition(), context.getMessageRecord().getId());
      context.setAcked(true);
    }
  }

  protected void fireMessageAcknowledgedEvent(TkmsShardPartition shardPartition, Long id, ProducerRecord<String, byte[]> producerRecord) {
    List<ITkmsEventsListener> listeners = getTkmsEventsListeners();
    if (log.isDebugEnabled()) {
      log.debug("Message was acknowledged for " + shardPartition + " with storage id " + id + ". Listeners count: " + listeners.size());
    }

    if (listeners.isEmpty()) {
      return;
    }

    listeners.forEach(tkmsEventsListener -> {
      try {
        tkmsEventsListener.messageAcknowledged(new MessageAcknowledgedEvent()
            .setShardPartition(shardPartition).setStorageId(id).setProducerRecord(producerRecord));
      } catch (Throwable t) {
        if (exceptionRateLimiter.tryAcquire()) {
          log.error(t.getMessage(), t);
        }
      }
    });
  }

  private ProducerRecord<String, byte[]> toProducerRecord(MessageRecord messageRecord) {
    StoredMessage.Message storedMessage = messageRecord.getMessage();

    List<Header> headers = null;
    if (storedMessage.hasHeaders() && storedMessage.getHeaders().getHeadersCount() > 0) {
      headers = new ArrayList<>();
      for (int i = 0; i < storedMessage.getHeaders().getHeadersCount(); i++) {
        StoredMessage.Header messageHeader = storedMessage.getHeaders().getHeaders(i);
        headers.add(new RecordHeader(messageHeader.getKey(), messageHeader.getValue().toByteArray()));
      }
    }

    //TODO: Should we support empty string keys?

    return new ProducerRecord<>(
        storedMessage.getTopic(), storedMessage.hasPartition() ? storedMessage.getPartition().getValue() : null,
        storedMessage.hasTimestamp() ? storedMessage.getTimestamp().getValue() : null,
        StringUtils.isEmpty(storedMessage.getKey()) ? null : storedMessage.getKey(), storedMessage.getValue().toByteArray(), headers);
  }

  @Override
  public void applicationStarted() {
    for (LeaderSelectorV2 leaderSelector : leaderSelectors) {
      leaderSelector.start();
    }
  }

  @Override
  public void prepareForShutdown() {
    for (LeaderSelectorV2 leaderSelector : leaderSelectors) {
      leaderSelector.stop();
    }
  }

  @Override
  public boolean canShutdown() {
    for (LeaderSelectorV2 leaderSelector : leaderSelectors) {
      if (!leaderSelector.hasStopped()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void applicationTerminating() {
    for (LeaderSelectorV2 leaderSelector : leaderSelectors) {
      if (!leaderSelector.hasStopped()) {
        IllegalStateException e = new IllegalStateException("All leader selectors were not terminated properly.");
        log.error(e.getMessage(), e);
        return;
      }
    }
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

  @TestOnly
  public void pause() {
    this.pauseRequested = true;
    this.paused = false;
  }

  /**
   * There is a small chance of race condition between pauseRequested and pause.
   *
   * <p>However this would only affect some tests with extremely low probability. So creating a new lock is not feasible.
   */
  @TestOnly
  public void resume() {
    this.pauseRequested = false;
    this.paused = false;
  }

  @TestOnly
  public boolean isPaused() {
    return paused;
  }

  @Data
  @Accessors(chain = true)
  protected static class MessageProcessingContext {

    private boolean acked;
    private Future<RecordMetadata> kafkaSenderFuture;
    private ProducerRecord<String, byte[]> producerRecord;
    private MessageRecord messageRecord;
    private TkmsShardPartition shardPartition;
  }
}
