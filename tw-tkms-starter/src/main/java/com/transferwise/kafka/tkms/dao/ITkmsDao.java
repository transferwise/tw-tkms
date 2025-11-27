package com.transferwise.kafka.tkms.dao;

import com.transferwise.kafka.tkms.TkmsMessageWithSequence;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.metrics.MonitoringQuery;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ITkmsDao {

  InsertMessageResult insertMessage(TkmsShardPartition shardPartition, TkmsMessage message);

  List<InsertMessageResult> insertMessages(TkmsShardPartition shardPartition, List<TkmsMessageWithSequence> tkmsMessages);

  @MonitoringQuery
  long getApproximateMessagesCount(TkmsShardPartition sp);

  @MonitoringQuery
  boolean hasMessagesBeforeId(TkmsShardPartition sp, Long messageId);

  @Data
  @Accessors(chain = true)
  class InsertMessageResult {

    private Long storageId;
    private int sequence;
    private TkmsShardPartition shardPartition;
  }

  List<MessageRecord> getMessages(TkmsShardPartition shardPartition, long earliestMessageId, int maxCount);

  /**
   * Retrieves messages using limit and offset for efficient pagination.
   *
   * @param shardPartition the shard partition to query
   * @param earliestMessageId the earliest message ID to start from (-1 for all messages)
   * @param limit the maximum number of messages to return
   * @param offset the number of records to skip
   * @return list of message records ordered by ID
   */
  List<MessageRecord> getMessages(TkmsShardPartition shardPartition, long earliestMessageId, int limit, int offset);

  @Data
  @Accessors(chain = true)
  class MessageRecord {

    private long id;
    private StoredMessage.Message message;
  }

  void deleteMessages(TkmsShardPartition shardPartition, List<Long> records);

  Long getEarliestMessageId(TkmsShardPartition shardPartition);

  void saveEarliestMessageId(TkmsShardPartition shardPartition, long messageId);

  boolean insertEarliestMessageId(TkmsShardPartition shardPartition);

  /**
   * Validates the database in general.
   *
   * <p>E.g. if index hints are supported.
   */
  void validateDatabase();

  /**
   * Validates the database for a shard.
   *
   * <p>E.g. if tables for a particular shard have been created.
   */
  void validateDatabase(int shard);

}
