package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptor;
import com.transferwise.kafka.tkms.api.TkmsProxyDecision;
import com.transferwise.kafka.tkms.api.TkmsProxyDecision.Result;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestMessagesIntereceptor implements ITkmsMessageInterceptor {
  public TkmsProxyDecision beforeProxy(ProducerRecord<String, byte[]> producerRecord) {
    return new TkmsProxyDecision().setResult(Result.NEUTRAL);
  }
  
  public TkmsProxyDecision onError(Throwable t, ProducerRecord<String, byte[]> producerRecord) {
    return new TkmsProxyDecision().setResult(Result.NEUTRAL);
  }

}
