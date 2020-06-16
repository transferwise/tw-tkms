package com.transferwise.kafka.tkms.api;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ProxyDecision {

  private Result result;

  public enum Result {
    RETRY,
    DISCARD,
    NEUTRAL
  }
}
