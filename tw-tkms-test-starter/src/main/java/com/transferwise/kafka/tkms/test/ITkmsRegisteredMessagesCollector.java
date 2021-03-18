package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ITkmsRegisteredMessagesCollector {

  void clear();

  <T> List<T> getRegisteredJsonMessages(String topic, Class<T> clazz);

  List<RegisteredMessage> getRegisteredMessages(String topic);

  @Data
  @Accessors(chain = true)
  class RegisteredMessage {

    private String storageId;
    private TkmsShardPartition shardPartition;
    private TkmsMessage message;
  }
}
