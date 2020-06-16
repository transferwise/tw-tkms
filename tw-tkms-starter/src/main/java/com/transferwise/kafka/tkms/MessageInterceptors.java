package com.transferwise.kafka.tkms;

import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.kafka.tkms.api.IMessageInterceptor;
import com.transferwise.kafka.tkms.api.IMessageInterceptors;
import com.transferwise.kafka.tkms.api.ProxyDecision;
import com.transferwise.kafka.tkms.api.ProxyDecision.Result;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

@Slf4j
public class MessageInterceptors implements IMessageInterceptors {

  @Autowired
  private ApplicationContext applicationContext;

  private List<IMessageInterceptor> messageInterceptors;
  private RateLimiter errorRateLimiter = RateLimiter.create(2);

  @Override
  public ProxyDecision beforeProxy(ProducerRecord<String, byte[]> message) {
    List<IMessageInterceptor> interceptors = getMessageInterceptors();
    if (interceptors != null) {
      for (IMessageInterceptor interceptor : interceptors) {
        try {
          ProxyDecision proxyDecision = interceptor.beforeProxy(message);
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
    return new ProxyDecision().setResult(Result.NEUTRAL);
  }

  protected List<IMessageInterceptor> getMessageInterceptors() {
    if (messageInterceptors == null) {
      messageInterceptors = new ArrayList<>(applicationContext.getBeansOfType(IMessageInterceptor.class).values());
    }
    return messageInterceptors;
  }
}
