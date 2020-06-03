package com.transferwise.kafka.tkms.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.api.ITkmsEventsListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;

public class SentMessagesCollector implements ISentMessagesCollector, ITkmsEventsListener {

  @Autowired
  private ObjectMapper objectMapper;
  @Autowired
  private TkmsTestProperties tkmsTestProperties;

  private Map<String, Map<Long, SentMessage>> messages = new ConcurrentHashMap<>();

  private AtomicInteger messagesCount = new AtomicInteger();

  @Override
  public void messageAcknowledged(MessageAcknowledgedEvent event) {
    if (messagesCount.get() >= tkmsTestProperties.getMaxCollectedMessages()) {
      throw new IllegalStateException(
          "Collected " + messagesCount.get() + " messages, while the limit is " + tkmsTestProperties.getMaxCollectedMessages());
    }
    messagesCount.incrementAndGet();
    messages.computeIfAbsent(event.getProducerRecord().topic(), (k) -> Collections.synchronizedMap(new LinkedHashMap<>()))
        .put(event.getId(), new SentMessage().setId(event.getId()).setProducerRecord(event.getProducerRecord()));
  }

  // Not fully atomic, but we don't care for high precision here.
  @Override
  public void clear() {
    messages = new ConcurrentHashMap<>();
    messagesCount.set(0);
  }

  @Override
  public <T> List<T> getSentJsonMessages(String topic, Class<T> clazz) {
    return getSentMessages(topic).stream().map(sm -> ExceptionUtils.doUnchecked(() -> objectMapper.readValue(sm.getProducerRecord().value(),
        clazz))).collect(Collectors.toList());
  }

  @Override
  public List<SentMessage> getSentMessages(String topic) {
    Map<Long, SentMessage> messagesInTopic = messages.get(topic);

    if (messagesInTopic == null) {
      return new ArrayList<>();
    }

    return new ArrayList<>(messagesInTopic.values());
  }
}
