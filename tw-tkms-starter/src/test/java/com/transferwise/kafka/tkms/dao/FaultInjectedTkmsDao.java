package com.transferwise.kafka.tkms.dao;

import com.transferwise.kafka.tkms.TkmsMessageWithSequence;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.util.List;
import lombok.Setter;

public class FaultInjectedTkmsDao implements ITkmsDao {

  private final ITkmsDao delegate;

  @Setter
  private boolean deleteMessagesFails;

  @Setter
  private Integer insertMessagesErrorLatch;

  public FaultInjectedTkmsDao(ITkmsDao delegate) {
    this.delegate = delegate;
  }

  @Override
  public InsertMessageResult insertMessage(TkmsShardPartition shardPartition, TkmsMessage message) {
    if (insertMessagesErrorLatch != null) {
      if (--insertMessagesErrorLatch < 0) {
        throw new IllegalStateException("Haha, inserts are failing lol.");
      }
    }

    return delegate.insertMessage(shardPartition, message);
  }

  @Override
  public List<InsertMessageResult> insertMessages(TkmsShardPartition shardPartition, List<TkmsMessageWithSequence> tkmsMessages) {
    if (insertMessagesErrorLatch != null) {
      if (--insertMessagesErrorLatch < 0) {
        throw new IllegalStateException("Haha, inserts are failing lol.");
      }
    }
    return delegate.insertMessages(shardPartition, tkmsMessages);
  }

  @Override
  public long getApproximateMessagesCount(TkmsShardPartition sp) {
    return delegate.getApproximateMessagesCount(sp);
  }

  @Override
  public boolean hasMessagesBeforeId(TkmsShardPartition sp, Long messageId) {
    return delegate.hasMessagesBeforeId(sp, messageId);
  }

  @Override
  public List<MessageRecord> getMessages(TkmsShardPartition shardPartition, long earliestMessageId, int maxCount) {
    return delegate.getMessages(shardPartition, earliestMessageId, maxCount);
  }

  @Override
  public List<MessageRecord> getMessages(TkmsShardPartition shardPartition, long earliestMessageId, int limit, int offset) {
    return delegate.getMessages(shardPartition, earliestMessageId, limit, offset);
  }

  @Override
  public void deleteMessages(TkmsShardPartition shardPartition, List<Long> records) {
    if (deleteMessagesFails) {
      throw new IllegalStateException("Delete messages has a bad day.");
    } else {
      delegate.deleteMessages(shardPartition, records);
    }
  }

  @Override
  public Long getEarliestMessageId(TkmsShardPartition shardPartition) {
    return delegate.getEarliestMessageId(shardPartition);
  }

  @Override
  public void saveEarliestMessageId(TkmsShardPartition shardPartition, long messageId) {
    delegate.saveEarliestMessageId(shardPartition, messageId);
  }

  @Override
  public boolean insertEarliestMessageId(TkmsShardPartition shardPartition) {
    return delegate.insertEarliestMessageId(shardPartition);
  }

  @Override
  public void validateDatabase() {
    delegate.validateDatabase();
  }

  @Override
  public void validateDatabase(int shard) {
    delegate.validateDatabase(shard);
  }

}
