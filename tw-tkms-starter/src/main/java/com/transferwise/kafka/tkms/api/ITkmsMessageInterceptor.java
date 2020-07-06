package com.transferwise.kafka.tkms.api;

import com.transferwise.kafka.tkms.api.TkmsProxyDecision.Result;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface ITkmsMessageInterceptor {

  /**
   * For emergency reasons.
   *
   * <p>Scenario is the following.
   *
   * <p>1. Some messages are stored, which can not be sent out. For example due to ACL issues. Over time, it starts blocking other messages.
   *
   * <p>2. Engineer can find a DBA to delete the messages, but he can also implement this method and tell the engine to discard those,
   * optionally saving those records by himself to a some kind of DLQ or a separate shard.
   */
  default TkmsProxyDecision beforeProxy(ProducerRecord<String, byte[]> producerRecord) {
    return new TkmsProxyDecision().setResult(Result.NEUTRAL);
  }

}
