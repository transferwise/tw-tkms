package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.ITkmsMessageDecorator;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsMessage.Header;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class TestMessageDecorator implements ITkmsMessageDecorator {

  @Override
  public List<Header> getHeaders(TkmsMessage message) {
    var h1 = new Header().setKey("tool").setValue("jambi".getBytes(StandardCharsets.UTF_8));
    if (message.getValue() != null && new String(message.getValue(), StandardCharsets.UTF_8).startsWith("Here from")) {
      return List.of(h1);
    }
    return List.of();
  }

}
