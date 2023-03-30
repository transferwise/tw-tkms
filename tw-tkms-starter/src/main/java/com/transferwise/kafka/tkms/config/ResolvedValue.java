package com.transferwise.kafka.tkms.config;

import com.transferwise.kafka.tkms.config.ResolvedValue.ResolvedValueValidator;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

@Documented
@Constraint(validatedBy = ResolvedValueValidator.class)
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ResolvedValue {

  String message() default "Unresolved value.";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};

  class ResolvedValueValidator implements ConstraintValidator<ResolvedValue, String> {

    @Override
    public void initialize(ResolvedValue contactNumber) {
      // Nothing to initialize
    }

    @Override
    public boolean isValid(String field, ConstraintValidatorContext ctx) {
      return field == null || !field.contains("${");
    }
  }
}