package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.concurrency.DefaultExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.kafka.tkms.TkmsProperties.DatabaseDialect;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.dao.TkmsDao;
import com.transferwise.kafka.tkms.dao.TkmsPostgresDao;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class TkmsAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public ITransactionalKafkaMessageSender tkmsSender() {
    return new TransactionalKafkaMessageSender();
  }

  @Bean
  @ConditionalOnMissingBean
  public ITkmsDao tkmsDao(TkmsProperties tkmsProperties) {
    if (tkmsProperties.getDatabaseDialect() == DatabaseDialect.POSTGRES) {
      return new TkmsPostgresDao();
    }
    return new TkmsDao();
  }

  @Bean
  @ConditionalOnMissingBean
  public IStorageToKafkaProxy tkmsStorageToKafkaProxy() {
    return new StorageToKafkaProxy();
  }

  @Bean
  @ConditionalOnMissingBean
  public IExecutorServicesProvider tkmsExecutorServicesProvider() {
    return new DefaultExecutorServicesProvider();
  }

  @Bean
  @ConditionalOnMissingBean
  @ConfigurationProperties(prefix = "tw-tmks", ignoreUnknownFields = false)
  public TkmsProperties tkmsProperties(Environment env) {
    TkmsProperties props = new TkmsProperties();
    props.setGroupId(env.getProperty("spring.application.name"));
    return props;
  }

  @Bean
  @ConditionalOnMissingBean
  public ITkmsPaceMaker tkmsPaceMaker(){
    return new TkmsPaceMaker();
  }

  @Bean
  @ConditionalOnMissingBean
  public ITkmsZookeeperOperations tkmsZookeeperOperations(){
    return new TkmsZookeeperOperations();
  }
}
