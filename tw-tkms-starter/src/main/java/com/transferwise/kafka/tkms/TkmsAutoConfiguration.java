package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.concurrency.DefaultExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.kafka.tkms.TkmsProperties.DatabaseDialect;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.Tkms;
import com.transferwise.kafka.tkms.api.helpers.ITkmsMessageFactory;
import com.transferwise.kafka.tkms.api.helpers.TkmsMessageFactory;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.dao.TkmsDao;
import com.transferwise.kafka.tkms.dao.TkmsPostgresDao;
import com.transferwise.kafka.tkms.metrics.IMetricsTemplate;
import com.transferwise.kafka.tkms.metrics.MetricsTemplate;
import io.micrometer.core.instrument.MeterRegistry;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
@Slf4j
public class TkmsAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public ITkmsMessageFactory tkmsMessageFactory() {
    return new TkmsMessageFactory();
  }

  @Bean
  @ConditionalOnMissingBean
  public IMetricsTemplate tkmsMetricsTemplate(MeterRegistry meterRegistry) {
    return new MetricsTemplate(meterRegistry);
  }

  @Bean
  @ConditionalOnMissingBean
  public ITransactionalKafkaMessageSender tkmsTransactionalKafkaMessageSender() {
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
  @ConfigurationProperties(prefix = "tw-tkms", ignoreUnknownFields = false)
  public TkmsProperties tkmsProperties(Environment env) {
    TkmsProperties props = new TkmsProperties();
    props.setGroupId(env.getProperty("spring.application.name"));
    return props;
  }

  @Bean
  @ConditionalOnMissingBean
  public ITkmsPaceMaker tkmsPaceMaker() {
    return new TkmsPaceMaker();
  }

  @Bean
  @ConditionalOnMissingBean
  public ITkmsZookeeperOperations tkmsZookeeperOperations() {
    return new TkmsZookeeperOperations();
  }

  @Bean
  @ConditionalOnMissingBean(TkmsDataSourceProvider.class)
  public TkmsDataSourceProvider tkmsDataSourceProvider(
      @Autowired(required = false) @Tkms DataSource dataSource, ConfigurableListableBeanFactory beanFactory) {
    if (dataSource == null) {
      String[] beanNames = beanFactory.getBeanNamesForType(DataSource.class);
      if (beanNames.length == 0) {
        throw new IllegalStateException("No DataSource bean(s) found.");
      } else if (beanNames.length == 1) {
        dataSource = beanFactory.getBean(beanNames[0], DataSource.class);
      } else {
        for (String beanName : beanNames) {
          BeanDefinition bd = beanFactory.getBeanDefinition(beanName);
          if (bd.isPrimary()) {
            dataSource = beanFactory.getBean(beanName, DataSource.class);
            break;
          }
        }
        if (dataSource == null) {
          throw new IllegalStateException(
              "" + beanNames.length + " data source(s) found, but none is marked as Primary nor qualified with @TwTkms: "
                  + String.join(", ", beanNames));
        }
      }
    }
    return new TkmsDataSourceProvider(dataSource);
  }

  public static class TkmsDataSourceProvider {

    private final DataSource dataSource;

    public TkmsDataSourceProvider(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
      return dataSource;
    }
  }
}
