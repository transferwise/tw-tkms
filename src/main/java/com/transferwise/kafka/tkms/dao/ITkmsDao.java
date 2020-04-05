package com.transferwise.kafka.tkms.dao;

import com.transferwise.kafka.tkms.Message;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import java.time.Instant;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ITkmsDao {

  long insertMessage(Message message);

  List<MessageRecord> getMessages(int shard, int maxCount);

  void deleteMessage(int shard, List<Long> records);

  @Data
  @Accessors(chain = true)
  class MessageRecord {

    private long id;
    private StoredMessage.Message message;
  }
}
