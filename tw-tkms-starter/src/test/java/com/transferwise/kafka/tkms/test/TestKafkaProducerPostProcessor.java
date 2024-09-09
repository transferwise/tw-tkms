package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.config.ITkmsKafkaProducerPostProcessor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

@Component
public class TestKafkaProducerPostProcessor implements ITkmsKafkaProducerPostProcessor {

  public static final byte[] TEST_MESSAGE = "Testing ProducerPostProcessing".getBytes(StandardCharsets.UTF_8);

  private ProxyInvocationHandler handler;

  @SuppressWarnings("unchecked")
  @Override
  public Producer<String, byte[]> apply(Producer<String, byte[]> producer) {
    handler = new ProxyInvocationHandler(producer);
    return (Producer<String, byte[]>)
        Proxy.newProxyInstance(
            TestKafkaProducerPostProcessor.class.getClassLoader(),
            new Class<?>[] {Producer.class},
            handler);
  }

  private static class ProxyInvocationHandler implements InvocationHandler {

    private final Producer<String, byte[]> producer;

    public ProxyInvocationHandler(Producer<String, byte[]> producer) {
      this.producer = producer;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if ("send".equals(method.getName())
          && method.getParameterCount() >= 1
          && method.getParameterTypes()[0] == ProducerRecord.class) {
        ProducerRecord<String, byte[]> record = (ProducerRecord<String, byte[]>) args[0];
        if (Arrays.equals(TEST_MESSAGE, record.value())) {
          record.headers().add("wrapTest", "wrapped".getBytes(StandardCharsets.UTF_8));
        }
        Callback callback =
            method.getParameterCount() >= 2
                && method.getParameterTypes()[1] == Callback.class
                ? (Callback) args[1]
                : null;
        return producer.send(record, callback);
      } else {
        try {
          return method.invoke(producer, args);
        } catch (InvocationTargetException exception) {
          throw exception.getCause();
        }
      }
    }
  }
}