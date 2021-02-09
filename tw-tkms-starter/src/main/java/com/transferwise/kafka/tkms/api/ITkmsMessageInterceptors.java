package com.transferwise.kafka.tkms.api;

import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptor.MessageInterceptionDecision;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface ITkmsMessageInterceptors {

  boolean hasInterceptors();

  /**
   * Aggregator for `ITkmsMessageInterceptor`.
   */
  Map<Integer, MessageInterceptionDecision> beforeSendingToKafka(@Nonnull Map<Integer, ProducerRecord<String, byte[]>> producerRecords);

  /**
   * Aggregator for `ITkmsMessageInterceptor`.
   */
  TkmsProxyDecision onError(Throwable t, ProducerRecord<String, byte[]> producerRecord);
}
