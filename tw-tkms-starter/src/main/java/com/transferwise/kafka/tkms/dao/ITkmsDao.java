package com.transferwise.kafka.tkms.dao;

import com.transferwise.kafka.tkms.TkmsMessageWithSequence;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ITkmsDao {

  InsertMessageResult insertMessage(TkmsShardPartition shardPartition, TkmsMessage message);

  List<InsertMessageResult> insertMessages(TkmsShardPartition shardPartition, List<TkmsMessageWithSequence> tkmsMessages);

  @Data
  @Accessors(chain = true)
  class InsertMessageResult {

    private Long storageId;
    private int sequence;
    private TkmsShardPartition shardPartition;
  }

  List<MessageRecord> getMessages(TkmsShardPartition shardPartition, int maxCount);

  void deleteMessages(TkmsShardPartition shardPartition, List<Long> records);

  @Data
  @Accessors(chain = true)
  class MessageRecord {

    private long id;
    private StoredMessage.Message message;
  }
}
