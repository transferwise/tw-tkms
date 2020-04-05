package com.transferwise.kafka.tkms;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("tw-tkms-test")
@Data
@Accessors(chain = true)
public class TestProperties {

  private String testTopic = "MyTestTopic";
}
