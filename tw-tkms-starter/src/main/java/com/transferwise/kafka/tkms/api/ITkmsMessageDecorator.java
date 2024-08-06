package com.transferwise.kafka.tkms.api;

import com.transferwise.kafka.tkms.api.TkmsMessage.Header;
import java.util.List;

public interface ITkmsMessageDecorator {

  default List<Header> getHeaders(TkmsMessage message) {
    return List.of();
  }
}
