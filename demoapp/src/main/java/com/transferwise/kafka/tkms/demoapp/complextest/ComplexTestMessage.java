package com.transferwise.kafka.tkms.demoapp.complextest;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ComplexTestMessage {

  private String topic;
  private Long entityId;
  private Long entitySeq;
}
