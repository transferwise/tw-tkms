package com.transferwise.kafka.tkms;

import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import org.springframework.beans.factory.annotation.Autowired;

public class LegacyEnvironmentValidator extends BaseEnvironmentValidator {

  @Autowired
  private Validator validator;

  @Override
  public void validate() {
    var violations = validator.validate(properties);
    if (!violations.isEmpty()) {
      throw new ConstraintViolationException(violations);
    }

    super.validate();
  }
}