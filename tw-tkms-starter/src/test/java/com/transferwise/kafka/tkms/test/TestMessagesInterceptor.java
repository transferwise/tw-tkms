package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptor;
import com.transferwise.kafka.tkms.api.TkmsProxyDecision;
import com.transferwise.kafka.tkms.api.TkmsProxyDecision.Result;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Setter;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestMessagesInterceptor implements ITkmsMessageInterceptor {
  @Setter
  private Function<Map<Integer, ProducerRecord<String, byte[]>>, Map<Integer, MessageInterceptionDecision>> beforeSendingToKafkaFunction;
  
  @Override
  public Map<Integer, MessageInterceptionDecision> beforeSendingToKafka(@Nonnull Map<Integer, ProducerRecord<String, byte[]>> producerRecords) {
    return beforeSendingToKafkaFunction == null ? null : beforeSendingToKafkaFunction.apply(producerRecords);
  }
  
  @Override
  public TkmsProxyDecision onError(Throwable t, ProducerRecord<String, byte[]> producerRecord) {
    return new TkmsProxyDecision().setResult(Result.NEUTRAL);
  }

}
