package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.common.leaderselector.Leader.Control;
import com.transferwise.common.leaderselector.LeaderSelector;
import com.transferwise.kafka.tkms.dao.ITkmsDao.MessageRecord;
import com.transferwise.kafka.tkms.dao.TkmsDao;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
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
  private MeterRegistry meterRegistry;
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;

  private final List<LeaderSelector> leaderSelectors = new ArrayList<>();

  @PostConstruct
  public void init() {
    for (int i = 0; i < properties.getShardsCount(); i++) {
      int shard = i;

      ExecutorService executorService =
          new ThreadNamingExecutorServiceWrapper("tw-tkms-poller-" + shard, executorServicesProvider.getGlobalExecutorService());

      leaderSelectors.add(new LeaderSelector(curatorFramework, zkOperations.getLockNodePath(shard), executorService, control -> {
        AtomicReference<Future<Boolean>> futureReference = new AtomicReference<>();

        control.workAsyncUntilShouldStop(() -> futureReference.set(executorService.submit(
            () -> {
              log.info("Starting to poll shard {}.", shard);
              poll(control, shard);
              return true;
            })),
            () -> {
              log.info("Stopping polling of shard {}.", shard);

              Future<Boolean> future = futureReference.get();
              if (future != null) {
                try {
                  Boolean result = future.get(tkmsPaceMaker.getLongWaitTime().toMillis(), TimeUnit.MILLISECONDS);
                  if (result == null) {
                    throw new IllegalStateException("Hang detected, when trying to stop polling of shard " + shard + ".");
                  }
                } catch (Throwable t) {
                  log.error(t.getMessage(), t);
                }
              }
            });
      }));
    }
  }

  private void poll(Control control, int shard) {
    while (!control.shouldStop()) {
      unitOfWorkManager.createEntryPoint("TKMS", "poll").toContext().execute(() -> {
        try {
          List<MessageRecord> records = dao.getMessages(shard, properties.getPollerBatchSize());
          Tags shardTag = Tags.of("shard", String.valueOf(shard));
          if (records.size() == 0) {
            meterRegistry.counter("tw.tkms.proxy.poll.empty", shardTag).increment();
            tkmsPaceMaker.doSmallPause();
            return;
          }
          meterRegistry.counter("tw.tkms.proxy.poll.notempty", shardTag).increment();
          byte[] success = new byte[records.size()];

          CountDownLatch latch = new CountDownLatch(records.size());
          for (int i = 0; i < records.size(); i++) {
            MessageRecord messageRecord = records.get(i);
            ProducerRecord<String, byte[]> producerRecord = toProducerRecord(messageRecord);

            int finalI = i;
            kafkaTemplate.send(producerRecord).addCallback(
                result -> {
                  success[finalI] = 1;
                  meterRegistry.counter("tw.tkms.proxy.sent", shardTag.and("topic", producerRecord.topic(), "success", "true")).increment();
                  latch.countDown();
                },
                ex -> {
                  log.error(ex.getMessage(), ex);
                  meterRegistry.counter("tw.tkms.proxy.sent.failed", shardTag.and("topic", producerRecord.topic(), "success", "false")).increment();
                  latch.countDown();
                });
          }
          latch.await();

          List<Long> successIds = new ArrayList<>();
          for (int i = 0; i < records.size(); i++) {
            if (success[i] == 1) {
              successIds.add(records.get(i).getId());
            }
          }
          //TODO: In current implementation this can create latency (but not reduce total throughput).
          // In the future we want to probably offload deleting into a separate thread(s)
          // Obviously a select then needs "where not id in(?,?,...)" and slowness from it has to be tested.
          dao.deleteMessage(shard, successIds);

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
}
