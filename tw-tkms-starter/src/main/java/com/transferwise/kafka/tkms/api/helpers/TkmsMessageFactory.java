package com.transferwise.kafka.tkms.api.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import java.nio.charset.StandardCharsets;
import javax.annotation.PostConstruct;
import lombok.NonNull;
import org.springframework.beans.factory.annotation.Autowired;

public class TkmsMessageFactory implements ITkmsMessageFactory {

  @Autowired(required = false)
  private ObjectMapper objectMapper;

  @PostConstruct
  public void init() {
    // Older Spring ecosystems may not have it created as global bean.
    if (objectMapper == null) {
      objectMapper = new ObjectMapper();
    }
  }

  @Override
  public TkmsMessage createJsonMessage(@NonNull Object value) {
    return ExceptionUtils.doUnchecked(() -> new TkmsMessage().setValue(objectMapper.writeValueAsBytes(value)));
  }

  @Override
  public TkmsMessage createTextMessage(@NonNull String value) {
    return new TkmsMessage().setValue(value.getBytes(StandardCharsets.UTF_8));
  }
}
