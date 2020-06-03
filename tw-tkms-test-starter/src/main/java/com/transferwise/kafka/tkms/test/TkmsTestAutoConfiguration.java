package com.transferwise.kafka.tkms.test;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TkmsTestAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public ISentMessagesCollector tkmsTestSentMessagesCollector() {
    return new SentMessagesCollector();
  }

  @Bean
  @ConditionalOnMissingBean
  public IRegisteredMessagesCollector tkmsTestRegisteredMessagesCollector() {
    return new RegisteredMessagesCollector();
  }
  
  @Bean
  @ConfigurationProperties(value = "tw-tkms.test", ignoreInvalidFields = true)
  public TkmsTestProperties tkmsTestProperties() {
    return new TkmsTestProperties();
  }
}
