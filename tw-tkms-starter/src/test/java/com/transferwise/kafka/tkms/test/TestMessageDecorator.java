package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.ITkmsMessageDecorator;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsMessage.Header;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
public class TestMessageDecorator implements ITkmsMessageDecorator {

  @Override
  public List<Header> getHeaders(TkmsMessage message){
    var h1 = new Header().setKey("adam-jones").setValue("jambi".getBytes());
    if (message.getValue() != null && new String(message.getValue()).startsWith("Here from the king")) {
      return List.of(h1);
    }
    return List.of();
  }

}
