package com.transferwise.kafka.tkms.test;

import java.util.ArrayList;
import java.util.List;
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

  private List<KafkaServer> kafkaServers = new ArrayList<>();

  @Data
  public static class KafkaServer {

    private String bootstrapServers;

    private List<String> topics = new ArrayList<>();
  }
}
