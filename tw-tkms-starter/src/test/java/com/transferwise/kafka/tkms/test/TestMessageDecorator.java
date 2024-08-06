package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.ITkmsMessageDecorator;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsMessage.Header;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class TestMessageDecorator implements ITkmsMessageDecorator {

  @Override
  public List<Header> getHeaders(TkmsMessage message) {
    var h1 = new Header().setKey("tool").setValue("jambi".getBytes());
    if (message.getKey() == null) {
      return List.of();
    }
    return List.of(h1);
  }

}
