package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface ITkmsSentMessagesCollector {

  void clear();

  <T> List<T> getSentJsonMessages(String topic, Class<T> clazz);

  List<SentMessage> getSentMessages(String topic);

  @Data
  @Accessors(chain = true)
  class SentMessage {

    private Long storageId;
    private TkmsShardPartition shardPartition;
    private ProducerRecord<String, byte[]> producerRecord;
  }
}
