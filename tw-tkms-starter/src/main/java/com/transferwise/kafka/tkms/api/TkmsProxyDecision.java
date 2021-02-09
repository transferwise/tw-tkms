package com.transferwise.kafka.tkms.api;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TkmsProxyDecision {

  private Result result;

  public enum Result {
    /**
     * Default, the message will be tried to be sent.
     */
    RETRY,
    /**
     * The message will get discarded.
     *
     * <p>Usually the code saves the message to it's own storage (e.g. DLQ), before answering with that.
     */
    DISCARD,
    /**
     * Let other interceptors make the final decision.
     *
     * <p>If all interceptors respond with NEUTRAL, a RETRY will be used.
     */
    NEUTRAL
  }
}
