package com.transferwise.kafka.tkms.dao;

import com.transferwise.kafka.tkms.ShardPartition;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ITkmsDao {

  InsertMessageResult insertMessage(int shard, TkmsMessage message);

  @Data
  @Accessors(chain = true)
  class InsertMessageResult {

    private Long id;
    private ShardPartition shardPartition;
  }

  List<MessageRecord> getMessages(ShardPartition shardPartition, int maxCount);

  void deleteMessage(ShardPartition shardPartition, List<Long> records);

  @Data
  @Accessors(chain = true)
  class MessageRecord {

    private long id;
    private StoredMessage.Message message;
  }
}
