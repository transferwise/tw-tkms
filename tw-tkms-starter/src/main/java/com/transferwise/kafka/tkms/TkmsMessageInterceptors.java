package com.transferwise.kafka.tkms;

import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptor;
import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptor.MessageInterceptionDecision;
import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptors;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

@Slf4j
public class TkmsMessageInterceptors implements ITkmsMessageInterceptors {

  @Autowired
  private ApplicationContext applicationContext;

  private List<ITkmsMessageInterceptor> messageInterceptors;
  private RateLimiter errorRateLimiter = RateLimiter.create(2);

  @Override
  public boolean hasInterceptors() {
    return !getMessageInterceptors().isEmpty();
  }

  @Override
  public Map<Integer, MessageInterceptionDecision> beforeSendingToKafka(@Nonnull TkmsShardPartition shardPartition,
      @Nonnull Map<Integer, ProducerRecord<String, byte[]>> producerRecords) {
    List<ITkmsMessageInterceptor> interceptors = getMessageInterceptors();
    if (interceptors.isEmpty()) {
      return null;
    }
    Map<Integer, MessageInterceptionDecision> result = new HashMap<>();
    producerRecords.forEach((k, v) -> result.put(k, MessageInterceptionDecision.NEUTRAL));

    for (ITkmsMessageInterceptor interceptor : interceptors) {
      Map<Integer, MessageInterceptionDecision> decisions = interceptor.beforeSendingToKafka(shardPartition, producerRecords);
      if (decisions != null) {
        result.forEach((k, v) -> {
          if (v == MessageInterceptionDecision.NEUTRAL) {
            MessageInterceptionDecision decision = decisions.get(k);
            if (decision != null && decision != MessageInterceptionDecision.NEUTRAL) {
              result.put(k, decision);
            }
          }
        });
      }
    }
    return result;
  }

  @Override
  public MessageInterceptionDecision onError(TkmsShardPartition tkmsShardPartition, Throwable t, ProducerRecord<String, byte[]> producerRecord) {
    List<ITkmsMessageInterceptor> interceptors = getMessageInterceptors();
    if (interceptors != null) {
      for (ITkmsMessageInterceptor interceptor : interceptors) {
        try {
          MessageInterceptionDecision proxyDecision = interceptor.onError(tkmsShardPartition, t, producerRecord);
          if (proxyDecision != null && proxyDecision != MessageInterceptionDecision.NEUTRAL) {
            return proxyDecision;
          }
        } catch (Throwable t1) {
          if (errorRateLimiter.tryAcquire()) {
            log.error(t1.getMessage(), t1);
          }
        }
      }
    }
    return MessageInterceptionDecision.NEUTRAL;
  }

  protected List<ITkmsMessageInterceptor> getMessageInterceptors() {
    if (messageInterceptors == null) {
      messageInterceptors = new ArrayList<>(applicationContext.getBeansOfType(ITkmsMessageInterceptor.class).values());
    }
    return messageInterceptors;
  }
}
