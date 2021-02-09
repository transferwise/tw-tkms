package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptor;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Setter;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestMessagesInterceptor implements ITkmsMessageInterceptor {

  @Setter
  private Function<Map<Integer, ProducerRecord<String, byte[]>>, Map<Integer, MessageInterceptionDecision>> beforeSendingToKafkaFunction;

  @Override
  public Map<Integer, MessageInterceptionDecision> beforeSendingToKafka(@Nonnull TkmsShardPartition shardPartition,
      @Nonnull Map<Integer, ProducerRecord<String, byte[]>> producerRecords) {
    return beforeSendingToKafkaFunction == null ? null : beforeSendingToKafkaFunction.apply(producerRecords);
  }

  @Override
  public MessageInterceptionDecision onError(@Nonnull TkmsShardPartition shardPartition, Throwable t, ProducerRecord<String, byte[]> producerRecord) {
    return MessageInterceptionDecision.NEUTRAL;
  }
}
