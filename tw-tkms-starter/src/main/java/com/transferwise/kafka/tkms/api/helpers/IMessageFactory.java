package com.transferwise.kafka.tkms.api.helpers;

import com.transferwise.kafka.tkms.api.Message;
import lombok.NonNull;

/**
 * Meant as quick helper to cover most use cases.
 * 
 * <p>However it is expected from any serious and large application to have it's own ways to convert various payloads to bytes.
 */
public interface IMessageFactory {

  Message createJsonMessage(@NonNull Object value);
  
  Message createTextMessage(@NonNull String value);
}
