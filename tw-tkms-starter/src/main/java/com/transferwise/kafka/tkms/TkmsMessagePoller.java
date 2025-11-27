package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsDaoProvider;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.dao.ITkmsDao.MessageRecord;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TkmsMessagePoller {

  private final ITkmsDaoProvider tkmsDaoProvider;
  private final IExecutorServicesProvider executorServicesProvider;
  private final int pollerParallelism;

  boolean useExecutors;

  public TkmsMessagePoller(ITkmsDaoProvider tkmsDaoProvider, IExecutorServicesProvider executorServicesProvider, int pollerParallelism) {
    this.tkmsDaoProvider = tkmsDaoProvider;
    this.executorServicesProvider = executorServicesProvider;
    this.pollerParallelism = pollerParallelism;
    this.useExecutors = pollerParallelism > 1;
  }

  public List<MessageRecord> pullMessages(
      TkmsShardPartition shardPartition,
      long earliestMessageId,
      int batchSize) {
    List<MessageRecord> messageRecords;
    if (useExecutors) {
      messageRecords = pullMessagesParallel(shardPartition, batchSize);
      useExecutors = messageRecords.size() > batchSize;
    } else {
      messageRecords = pullMessagesSequential(shardPartition, earliestMessageId, batchSize);
      useExecutors = messageRecords.size() == batchSize && pollerParallelism > 1;
    }
    return messageRecords;
  }

  private List<MessageRecord> pullMessagesSequential(
      TkmsShardPartition shardPartition,

      long earliestMessageId,
      int batchSize) {

    ITkmsDao tkmsDao = tkmsDaoProvider.getTkmsDao(shardPartition.getShard());
    return tkmsDao.getMessages(shardPartition, earliestMessageId, batchSize);
  }

  private List<MessageRecord> pullMessagesParallel(
      TkmsShardPartition shardPartition,
      int batchSize) {

    ITkmsDao tkmsDao = tkmsDaoProvider.getTkmsDao(shardPartition.getShard());
    Long minMessageId = tkmsDao.getMinMessageId(shardPartition);
    Long maxMessageId = tkmsDao.getMaxMessageId(shardPartition);
    long minId = minMessageId != null ? minMessageId : 0;
    long maxId = maxMessageId != null ? maxMessageId : 0;
    List<CompletableFuture<List<MessageRecord>>> futures = new ArrayList<>();
    for (int i = 0; i < pollerParallelism; i++) {
      final int offset = i * batchSize;
      final int limit = batchSize;
      futures.add(CompletableFuture.supplyAsync(
          () -> tkmsDao.getMessages(shardPartition, minId, limit, offset),
          executorServicesProvider.getGlobalExecutorService()));
      long lastLimit =  minId + (long) (i + 1) * batchSize;
      if (lastLimit > maxId) {
        break;
      }
    }

    List<MessageRecord> allRecords = new ArrayList<>();
    for (CompletableFuture<List<MessageRecord>> future : futures) {
      allRecords.addAll(future.join());
    }

    return allRecords.stream().sorted(Comparator.comparing(MessageRecord::getId)).collect(Collectors.toList());
  }
}
