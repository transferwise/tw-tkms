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

}
