package com.transferwise.kafka.tkms.api.helpers;

import com.transferwise.kafka.tkms.api.TkmsMessage;
import lombok.NonNull;

/**
 * A quick helper to cover most use cases.
 *
 * <p>However it is expected from any serious and large application to have it's own ways to convert various payloads to bytes.
 */
public interface ITkmsMessageFactory {

  /**
   * Creates a message with json payload.
   *
   * <p>Usually by using global Jackson ObjectMapper bean.
   */
  <T> TkmsMessage createJsonMessage(@NonNull T value);

  /**
   * Create a text message in UTF-8 encoding.
   */
  TkmsMessage createTextMessage(@NonNull String value);
}
