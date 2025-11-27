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
public class TkmsMessagePooler {

  private final ITkmsDaoProvider tkmsDaoProvider;
  private final IExecutorServicesProvider executorServicesProvider;

  public TkmsMessagePooler(ITkmsDaoProvider tkmsDaoProvider, IExecutorServicesProvider executorServicesProvider) {
    this.tkmsDaoProvider = tkmsDaoProvider;
    this.executorServicesProvider = executorServicesProvider;
  }

  public List<MessageRecord> pullMessages(
      TkmsShardPartition shardPartition,
      long earliestMessageId,
      int batchSize, int parallelism) {

    if (parallelism > 1) {
      return pullMessagesParallel(shardPartition, earliestMessageId, batchSize, parallelism);
    } else {
      return pullMessagesSequential(shardPartition, earliestMessageId, batchSize);
    }
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
      long earliestMessageId,
      int batchSize,
      int parallelism) {

    ITkmsDao tkmsDao = tkmsDaoProvider.getTkmsDao(shardPartition.getShard());

    List<CompletableFuture<List<MessageRecord>>> futures = new ArrayList<>();
    for (int i = 0; i < parallelism; i++) {
      final int offset = i * batchSize;
      final int limit = batchSize;

      futures.add(CompletableFuture.supplyAsync(
          () -> tkmsDao.getMessages(shardPartition, earliestMessageId, limit, offset),
          executorServicesProvider.getGlobalExecutorService()));
    }

    List<MessageRecord> allRecords = new ArrayList<>();
    for (CompletableFuture<List<MessageRecord>> future : futures) {
      allRecords.addAll(future.join());
    }

    return allRecords.stream().sorted(Comparator.comparing(MessageRecord::getId)).collect(Collectors.toList());
  }
}
