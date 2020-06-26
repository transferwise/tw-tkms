package com.transferwise.kafka.tkms;

import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.common.leaderselector.ILock;
import com.transferwise.common.leaderselector.Leader.Control;
import com.transferwise.common.leaderselector.LeaderSelectorV2;
import com.transferwise.common.leaderselector.SharedReentrantLockBuilderFactory;
import com.transferwise.kafka.tkms.api.IMessageInterceptors;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener.MessageAcknowledgedEvent;
import com.transferwise.kafka.tkms.api.ProxyDecision;
import com.transferwise.kafka.tkms.api.ProxyDecision.Result;
import com.transferwise.kafka.tkms.api.ShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.dao.ITkmsDao.MessageRecord;
import com.transferwise.kafka.tkms.metrics.IMetricsTemplate;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

@Slf4j
public class StorageToKafkaProxy implements GracefulShutdownStrategy, IStorageToKafkaProxy {

  @Autowired
  private ITkmsKafkaProducerProvider tkmsKafkaProducerProvider;
  @Autowired
  private IExecutorServicesProvider executorServicesProvider;
  @Autowired
  private TkmsProperties properties;
  @Autowired
  private ITkmsDao dao;
  @Autowired
  private ITkmsPaceMaker tkmsPaceMaker;
  @Autowired
  private ITkmsZookeeperOperations zkOperations;
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;
  @Autowired
  private IMetricsTemplate metricsTemplate;
  @Autowired
  private ApplicationContext applicationContext;
  @Autowired
  private IMessageInterceptors messageIntereceptors;
  @Autowired
  private SharedReentrantLockBuilderFactory lockBuilderFactory;

  private volatile List<ITkmsEventsListener> tkmsEventsListeners;
  private final List<LeaderSelectorV2> leaderSelectors = new ArrayList<>();
  private RateLimiter exceptionRateLimiter = RateLimiter.create(2);


  @PostConstruct
  public void init() {
    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(s); p++) {
        ShardPartition shardPartition = ShardPartition.of(s, p);

        ExecutorService executorService =
            new ThreadNamingExecutorServiceWrapper("tw-tkms-poller-" + s + "_" + p, executorServicesProvider.getGlobalExecutorService());

        ILock lock = lockBuilderFactory.createBuilder(zkOperations.getLockNodePath(shardPartition)).build();

        leaderSelectors.add(new LeaderSelectorV2.Builder().setLock(lock).setExecutorService(executorService).setLeader(control -> {
          AtomicReference<Future<Boolean>> futureReference = new AtomicReference<>();

          control.workAsyncUntilShouldStop(() -> futureReference.set(executorService.submit(
              () -> {
                try {
                  log.info("Starting to proxy {}.", shardPartition);
                  poll(control, shardPartition);
                  return true;
                } catch (Throwable t) {
                  log.error(t.getMessage(), t);
                  return false;
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
              });
        }).build());
      }
    }
  }

  private void poll(Control control, ShardPartition shardPartition) {
    int pollerBatchSize = properties.getPollerBatchSize(shardPartition.getShard());
    long startTimeMs = ClockHolder.getClock().millis();

    long timeToLiveMs = properties.getProxyTimeToLive().toMillis() + ThreadLocalRandom.current().nextLong(TimeUnit.SECONDS.toMillis(5));

    while (!control.shouldStop()) {
      if (ClockHolder.getClock().millis() - startTimeMs > timeToLiveMs) {
        // Poor man's balancer. Allow other nodes a chance to get a leader as well.
        // TODO: investigate how Kafka client does it and replicate.
        control.yield();
        return;
      }
      unitOfWorkManager.createEntryPoint("TKMS", "poll_" + shardPartition.getShard() + "_" + shardPartition.getPartition()).toContext()
          .execute(() -> {
            long cycleStartTimeMs = ClockHolder.getClock().millis();
            try {
              List<MessageRecord> records = dao.getMessages(shardPartition, pollerBatchSize);
              if (records.size() == 0) {
                metricsTemplate.recordProxyPoll(shardPartition, 0, cycleStartTimeMs);
                tkmsPaceMaker.doSmallPause(shardPartition.getShard());
                return;
              }
              metricsTemplate.recordProxyPoll(shardPartition, records.size(), cycleStartTimeMs);

              byte[] acks = new byte[records.size()];

              List<Future<RecordMetadata>> futures = new ArrayList<>();

              final long kafkaSendStartTimeMs = ClockHolder.getClock().millis();
              KafkaProducer<String, byte[]> kafkaProducer = tkmsKafkaProducerProvider.getKafkaProducer(shardPartition.getShard());

              for (int i = 0; i < records.size(); i++) {
                int finalI = i;

                MessageRecord messageRecord = records.get(i);
                ProducerRecord<String, byte[]> producerRecord = toProducerRecord(messageRecord);

                ProxyDecision proxyDecision = messageIntereceptors.beforeProxy(producerRecord);
                if (proxyDecision != null && proxyDecision.getResult() == Result.DISCARD) {
                  acks[finalI] = 1;
                  continue;
                }

                try {
                  Future<RecordMetadata> future = kafkaProducer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                      acks[finalI] = 1;
                      fireMessageAcknowledgedEvent(messageRecord.getId(), producerRecord);
                      Instant insertTime = messageRecord.getMessage().hasInsertTimestamp()
                          ? Instant.ofEpochMilli(messageRecord.getMessage().getInsertTimestamp().getValue()) : null;
                      metricsTemplate.recordProxyMessageSendSuccess(shardPartition, producerRecord.topic(), insertTime);
                    } else {
                      log.error("Sending message " + messageRecord.getId() + " in " + shardPartition + " failed.", exception);
                      metricsTemplate.recordProxyMessageSendFailure(shardPartition, producerRecord.topic());
                    }
                  });

                  futures.add(future);
                } catch (Throwable t) {
                  log.error("Sending message " + messageRecord.getId() + " in " + shardPartition + " failed.", t);
                }
              }

              if (!futures.isEmpty()) {
                kafkaProducer.flush();
              }

              for (Future<RecordMetadata> future : futures) {
                try {
                  future.get();
                } catch (Throwable t) {
                  log.error("Sending message failed.", t);
                }
              }

              metricsTemplate.recordProxyKafkaMessagesSend(shardPartition, kafkaSendStartTimeMs);

              List<Long> successIds = new ArrayList<>();
              for (int i = 0; i < records.size(); i++) {
                if (acks[i] == 1) {
                  successIds.add(records.get(i).getId());
                }
              }
              //TODO: In current implementation this can create latency (but not reduce total throughput).
              // In the future we may provide more algorithms here.
              //   For example we want to probably offload deleting into a separate thread(s)
              //   Select would need id>X, which probably would not be too bad.
              long deleteStartTimeMs = ClockHolder.getClock().millis();
              dao.deleteMessage(shardPartition, successIds);
              metricsTemplate.recordProxyMessagesDeletion(shardPartition, deleteStartTimeMs);

              if (successIds.size() != records.size()) {
                tkmsPaceMaker.pauseOnError(shardPartition.getShard());
              }
            } catch (Throwable t) {
              log.error(t.getMessage(), t);
              tkmsPaceMaker.pauseOnError(shardPartition.getShard());
            } finally {
              metricsTemplate.recordProxyCycle(shardPartition, cycleStartTimeMs);
            }
          });
    }
  }

  protected void fireMessageAcknowledgedEvent(Long id, ProducerRecord<String, byte[]> producerRecord) {
    List<ITkmsEventsListener> listeners = getTkmsEventsListeners();

    if (listeners.isEmpty()) {
      return;
    }
    listeners.forEach(tkmsEventsListener -> {
      try {
        tkmsEventsListener.messageAcknowledged(new MessageAcknowledgedEvent().setId(id).setProducerRecord(producerRecord));
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

}
