package com.transferwise.kafka.tkms;

import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.common.leaderselector.Leader.Control;
import com.transferwise.common.leaderselector.LeaderSelector;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener.MessageAcknowledgedEvent;
import com.transferwise.kafka.tkms.dao.ITkmsDao.MessageRecord;
import com.transferwise.kafka.tkms.dao.TkmsDao;
import com.transferwise.kafka.tkms.metrics.IMetricsTemplate;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public class StorageToKafkaProxy implements GracefulShutdownStrategy, IStorageToKafkaProxy {

  @Autowired
  private KafkaTemplate<String, byte[]> kafkaTemplate;
  @Autowired
  private IExecutorServicesProvider executorServicesProvider;
  @Autowired
  private CuratorFramework curatorFramework;
  @Autowired
  private TkmsProperties properties;
  @Autowired
  private TkmsDao dao;
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

  private volatile List<ITkmsEventsListener> tkmsEventsListeners;
  private final List<LeaderSelector> leaderSelectors = new ArrayList<>();
  private RateLimiter exceptionRateLimiter = RateLimiter.create(2);

  @PostConstruct
  public void init() {
    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(); p++) {
        ShardPartition shardPartition = ShardPartition.of(s, p);

        ExecutorService executorService =
            new ThreadNamingExecutorServiceWrapper("tw-tkms-poller-" + s + "_" + p, executorServicesProvider.getGlobalExecutorService());

        leaderSelectors.add(new LeaderSelector(curatorFramework, zkOperations.getLockNodePath(shardPartition), executorService, control -> {
          AtomicReference<Future<Boolean>> futureReference = new AtomicReference<>();

          control.workAsyncUntilShouldStop(() -> futureReference.set(executorService.submit(
              () -> {
                log.info("Starting to proxy {}.", shardPartition);
                poll(control, shardPartition);
                return true;
              })),
              () -> {
                log.info("Stopping proxying for {}.", shardPartition);

                Future<Boolean> future = futureReference.get();
                if (future != null) {
                  try {
                    Boolean result = future.get(tkmsPaceMaker.getLongWaitTime().toMillis(), TimeUnit.MILLISECONDS);
                    if (result == null) {
                      throw new IllegalStateException("Hang detected when trying to stop polling of " + shardPartition + ".");
                    }
                  } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                  }
                }
              });
        }));
      }
    }
  }

  private void poll(Control control, ShardPartition shardPartition) {
    while (!control.shouldStop()) {
      unitOfWorkManager.createEntryPoint("TKMS", "poll_" + shardPartition.getShard() + "_" + shardPartition.getPartition()).toContext()
          .execute(() -> {
            try {
              long startTime = ClockHolder.getClock().millis();
              List<MessageRecord> records = dao.getMessages(shardPartition, properties.getPollerBatchSize());
              if (records.size() == 0) {
                metricsTemplate.registerProxyPoll(shardPartition, 0, startTime);
                tkmsPaceMaker.doSmallPause();
                return;
              }
              metricsTemplate.registerProxyPoll(shardPartition, records.size(), startTime);

              byte[] acks = new byte[records.size()];

              CountDownLatch latch = new CountDownLatch(records.size());
              for (int i = 0; i < records.size(); i++) {
                MessageRecord messageRecord = records.get(i);
                ProducerRecord<String, byte[]> producerRecord = toProducerRecord(messageRecord);

                int finalI = i;
                kafkaTemplate.send(producerRecord).addCallback(
                    result -> {
                      acks[finalI] = 1;
                      fireMessageAcknowledgedEvent(messageRecord.getId(), producerRecord);
                      metricsTemplate.registerProxyMessageSent(shardPartition, producerRecord.topic(), true);
                      latch.countDown();
                    },
                    ex -> {
                      //TODO: This can halt the whole system, if it's not a recoverable error. For example when the topic does not exist and
                      //      can not be auto created.
                      //      We can do 2 things here.
                      //      1. Allow an interception point for application, through some kind of filter/interceptor bean.
                      //      This filter can get the tries count and return an indication what to do: discard, move to dlq table, try again.
                      //      2. Implement a DLQ table.
                      
                      log.error("Sending message " + messageRecord.getId() + " in " + shardPartition + " failed.", ex);
                      metricsTemplate.registerProxyMessageSent(shardPartition, producerRecord.topic(), false);
                      latch.countDown();
                    });
              }
              latch.await();

              List<Long> successIds = new ArrayList<>();
              for (int i = 0; i < records.size(); i++) {
                if (acks[i] == 1) {
                  successIds.add(records.get(i).getId());
                }
              }
              //TODO: In current implementation this can create latency (but not reduce total throughput).
              // In the future we want to probably offload deleting into a separate thread(s)
              // Obviously a select then needs "where not id in(?,?,...)" and slowness from it has to be tested.
              dao.deleteMessage(shardPartition, successIds);

              if (successIds.size() != records.size()) {
                tkmsPaceMaker.pauseOnError();
              }
            } catch (Throwable t) {
              log.error(t.getMessage(), t);
              tkmsPaceMaker.pauseOnError();
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

    //TODO: Should we support emtpy string keys?

    return new ProducerRecord<>(
        storedMessage.getTopic(), storedMessage.hasPartition() ? storedMessage.getPartition().getValue() : null,
        storedMessage.hasTimestamp() ? storedMessage.getTimestamp().getValue() : null,
        StringUtils.isEmpty(storedMessage.getKey()) ? null : storedMessage.getKey(), storedMessage.getValue().toByteArray(), headers);
  }

  @Override
  public void applicationStarted() {
    for (LeaderSelector leaderSelector : leaderSelectors) {
      leaderSelector.start();
    }
  }

  @Override
  public void prepareForShutdown() {
    for (LeaderSelector leaderSelector : leaderSelectors) {
      leaderSelector.stop();
    }
  }

  @Override
  public boolean canShutdown() {
    for (LeaderSelector leaderSelector : leaderSelectors) {
      if (!leaderSelector.hasStopped()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void applicationTerminating() {
    for (LeaderSelector leaderSelector : leaderSelectors) {
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
