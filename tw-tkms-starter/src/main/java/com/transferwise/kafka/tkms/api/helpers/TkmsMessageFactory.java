package com.transferwise.kafka.tkms.api.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import lombok.NonNull;
import org.springframework.beans.factory.annotation.Autowired;

public class TkmsMessageFactory implements ITkmsMessageFactory {

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private Validator validator;

  @Override
  public <T> TkmsMessage createJsonMessage(@NonNull T value) {
    // It's important to fail fast on producer side, so we will enforce validation in default implementation.
    Set<ConstraintViolation<T>> violations = validator.validate(value);
    if (!violations.isEmpty()) {
      throw new ConstraintViolationException(violations);
    }

    return ExceptionUtils.doUnchecked(() -> new TkmsMessage().setValue(objectMapper.writeValueAsBytes(value)));
  }

  @Override
  public TkmsMessage createTextMessage(@NonNull String value) {
    return new TkmsMessage().setValue(value.getBytes(StandardCharsets.UTF_8));
  }
}
