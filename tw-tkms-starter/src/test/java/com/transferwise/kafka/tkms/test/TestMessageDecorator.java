package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.ITkmsMessageDecorator;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsMessage.Header;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class TestMessageDecorator implements ITkmsMessageDecorator {

  @Override
  public List<Header> getAdditionalHeaders(TkmsMessage message) {
    if (message.getValue() != null && new String(message.getValue(), StandardCharsets.UTF_8).startsWith("Here from")) {
      return List.of(new Header().setKey("tool").setValue("jambi".getBytes(StandardCharsets.UTF_8)));
    }
    return List.of();
  }

  @Override
  public TkmsShardPartition getOverridedPartition(TkmsMessage message) {
    if (message.getValue() != null && new String(message.getValue(), StandardCharsets.UTF_8).startsWith("Here from")) {
      return new TkmsShardPartition(0, 0);
    }
    return null;
  }

}
