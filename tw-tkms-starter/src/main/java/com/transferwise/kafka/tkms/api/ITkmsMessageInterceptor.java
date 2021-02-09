package com.transferwise.kafka.tkms.api;

import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface ITkmsMessageInterceptor {

  /**
   * The method is called, before we batch-send out those producerRecords.
   *
   * <p>Care should be taken to avoid throwing error from this method. However when it happens, all messages will be tried to be intercepted again
   * shortly after.
   *
   * <p>The method has to produce a map, which contains a decision for every input map's key.
   *
   * <p>The input keys (integers) are only meaningful in context of one method call. Same kafka message can have and almost always has a different
   * key for every call.
   */
  default Map<Integer, MessageInterceptionDecision> beforeSendingToKafka(@Nonnull Map<Integer, ProducerRecord<String, byte[]>> producerRecords) {
    return null;
  }

  /**
   * When an error happens, the interceptor can decide what to do with the message.
   *
   * <p>By default we will be retrying sending that message until it succeeds.
   */
  default MessageInterceptionDecision onError(Throwable t, ProducerRecord<String, byte[]> producerRecord) {
    return MessageInterceptionDecision.NEUTRAL;
  }

  enum MessageInterceptionDecision {
    /**
     * Default, the message will be tried to be sent.
     */
    RETRY,
    /**
     * The message will get discarded.
     *
     * <p>Usually the code saves the message to it's own storage (e.g. DLQ), before answering with that.
     */
    DISCARD,
    /**
     * Let other interceptors make the final decision.
     *
     * <p>If all interceptors respond with NEUTRAL, a RETRY will be used.
     */
    NEUTRAL
  }
}
