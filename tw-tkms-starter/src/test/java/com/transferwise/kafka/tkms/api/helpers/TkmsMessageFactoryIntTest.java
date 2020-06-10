package com.transferwise.kafka.tkms.api.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.transferwise.kafka.tkms.test.BaseTestEnvironment;
import javax.validation.ConstraintViolationException;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@BaseTestEnvironment
public class TkmsMessageFactoryIntTest {

  @Autowired
  private ITkmsMessageFactory tkmsMessageFactory;

  @Test
  public void testObjectValidation() {
    assertThatThrownBy(() -> tkmsMessageFactory.createJsonMessage(new Message())).isInstanceOf(ConstraintViolationException.class);

    assertThat(tkmsMessageFactory.createJsonMessage(new Message().setValue("Me"))).isNotNull();
  }

  @Data
  @Accessors(chain = true)
  protected static class Message {

    @NotNull
    private String value;
  }
}
