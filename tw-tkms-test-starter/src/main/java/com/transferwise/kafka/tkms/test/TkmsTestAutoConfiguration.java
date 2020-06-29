package com.transferwise.kafka.tkms.test;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TkmsTestAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public TkmsSentMessagesCollector tkmsTestSentMessagesCollector() {
    return new TkmsSentMessagesCollector();
  }

  @Bean
  @ConditionalOnMissingBean
  public TkmsRegisteredMessagesCollector tkmsTestRegisteredMessagesCollector() {
    return new TkmsRegisteredMessagesCollector();
  }

  @Bean
  @ConfigurationProperties(value = "tw-tkms.test", ignoreInvalidFields = true)
  @ConditionalOnMissingBean
  public TkmsTestProperties tkmsTestProperties() {
    return new TkmsTestProperties();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsTestDao.class)
  public TkmsTestDao tkmsTestDao() {
    return new TkmsTestDao();
  }
}
