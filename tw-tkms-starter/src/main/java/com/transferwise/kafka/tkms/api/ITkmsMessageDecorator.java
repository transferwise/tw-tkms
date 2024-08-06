package com.transferwise.kafka.tkms.api;

import com.transferwise.kafka.tkms.api.TkmsMessage.Header;
import java.util.List;
import org.springframework.core.Ordered;

public interface ITkmsMessageDecorator extends Ordered {

  default List<Header> getAdditionalHeaders(TkmsMessage message) {
    return List.of();
  }

  default TkmsShardPartition getOverridedPartition(TkmsMessage message) {
    return null;
  }

  default int getOrder() {
    return LOWEST_PRECEDENCE;
  }

}
