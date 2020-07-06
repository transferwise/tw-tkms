package com.transferwise.kafka.tkms;

import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptor;
import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptors;
import com.transferwise.kafka.tkms.api.TkmsProxyDecision;
import com.transferwise.kafka.tkms.api.TkmsProxyDecision.Result;
import java.util.ArrayList;
import java.util.List;
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
  public TkmsProxyDecision beforeProxy(ProducerRecord<String, byte[]> message) {
    List<ITkmsMessageInterceptor> interceptors = getMessageInterceptors();
    if (interceptors != null) {
      for (ITkmsMessageInterceptor interceptor : interceptors) {
        try {
          TkmsProxyDecision proxyDecision = interceptor.beforeProxy(message);
          if (proxyDecision != null && proxyDecision.getResult() != Result.NEUTRAL) {
            return proxyDecision;
          }
        } catch (Throwable t) {
          if (errorRateLimiter.tryAcquire()) {
            log.error(t.getMessage(), t);
          }
        }
      }
    }
    return new TkmsProxyDecision().setResult(Result.NEUTRAL);
  }

  protected List<ITkmsMessageInterceptor> getMessageInterceptors() {
    if (messageInterceptors == null) {
      messageInterceptors = new ArrayList<>(applicationContext.getBeansOfType(ITkmsMessageInterceptor.class).values());
    }
    return messageInterceptors;
  }
}
