package com.transferwise.kafka.tkms.test;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TkmsTestAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public ITkmsSentMessagesCollector tkmsTestSentMessagesCollector() {
    return new TkmsSentMessagesCollector();
  }

  @Bean
  @ConditionalOnMissingBean
  public ITkmsRegisteredMessagesCollector tkmsTestRegisteredMessagesCollector() {
    return new TkmsRegisteredMessagesCollector();
  }
  
  @Bean
  @ConfigurationProperties(value = "tw-tkms.test", ignoreInvalidFields = true)
  public TkmsTestProperties tkmsTestProperties() {
    return new TkmsTestProperties();
  }
}
