package com.transferwise.kafka.tkms.api.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.api.Message;
import java.nio.charset.StandardCharsets;
import lombok.NonNull;
import org.springframework.beans.factory.annotation.Autowired;

public class MessageFactory implements IMessageFactory {

  @Autowired
  private ObjectMapper objectMapper;

  @Override
  public Message createJsonMessage(@NonNull Object value) {
    return ExceptionUtils.doUnchecked(() -> new Message().setValue(objectMapper.writeValueAsBytes(value)));
  }

  @Override
  public Message createTextMessage(@NonNull String value) {
    return new Message().setValue(value.getBytes(StandardCharsets.UTF_8));
  }
}
