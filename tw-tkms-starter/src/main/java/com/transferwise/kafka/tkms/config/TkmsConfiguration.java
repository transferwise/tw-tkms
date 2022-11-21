package com.transferwise.kafka.tkms.config;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.*;
import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptors;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.Tkms;
import com.transferwise.kafka.tkms.api.helpers.ITkmsMessageFactory;
import com.transferwise.kafka.tkms.api.helpers.TkmsMessageFactory;
import com.transferwise.kafka.tkms.config.TkmsProperties.DatabaseDialect;
import com.transferwise.kafka.tkms.dao.*;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.metrics.TkmsClusterWideStateMonitor;
import com.transferwise.kafka.tkms.metrics.TkmsMetricsTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;

/**
 * Separated things here, which can be included in non spring-boot application without modification.
 */
@Configuration
@Slf4j
public class TkmsConfiguration {

  @Bean
  @ConditionalOnMissingBean(ITkmsMessageFactory.class)
  public TkmsMessageFactory tkmsMessageFactory() {
    return new TkmsMessageFactory();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsMetricsTemplate.class)
  public TkmsMetricsTemplate tkmsMetricsTemplate(IMeterCache meterCache) {
    return new TkmsMetricsTemplate(meterCache);
  }

  @Bean
  @ConditionalOnMissingBean(ITransactionalKafkaMessageSender.class)
  public TransactionalKafkaMessageSender tkmsTransactionalKafkaMessageSender() {
    return new TransactionalKafkaMessageSender();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsDao.class)
  public TkmsDao tkmsDao(ITkmsDataSourceProvider dataSourceProvider, TkmsProperties tkmsProperties,
                         ITkmsMetricsTemplate metricsTemplate, ITkmsMessageSerializer messageSerializer,
                         ITransactionsHelper transactionsHelper) {
    if (tkmsProperties.getDatabaseDialect() == DatabaseDialect.POSTGRES) {
      return new TkmsPostgresDao(dataSourceProvider, tkmsProperties, metricsTemplate, messageSerializer,transactionsHelper);
    }
    return new TkmsMariaDao(dataSourceProvider, tkmsProperties, metricsTemplate, messageSerializer,transactionsHelper);
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsStorageToKafkaProxy.class)
  public TkmsStorageToKafkaProxy tkmsStorageToKafkaProxy() {
    return new TkmsStorageToKafkaProxy();
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
  @ConditionalOnMissingBean(ITkmsPaceMaker.class)
  public TkmsPaceMaker tkmsPaceMaker() {
    return new TkmsPaceMaker();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsZookeeperOperations.class)
  public TkmsZookeeperOperations tkmsZookeeperOperations() {
    return new TkmsZookeeperOperations();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsDataSourceProvider.class)
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
              "" + beanNames.length + " data source(s) found, but none is marked as Primary nor qualified with @Tkms: "
                  + String.join(", ", beanNames));
        }
      }
    }
    return new TkmsDataSourceProvider(dataSource);
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsKafkaProducerProvider.class)
  public TkmsKafkaProducerProvider tkmsKafkaProducerProvider() {
    return new TkmsKafkaProducerProvider();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsMessageInterceptors.class)
  public TkmsMessageInterceptors tkmsMessageInterceptors() {
    return new TkmsMessageInterceptors();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsMessageSerializer.class)
  public TkmsMessageSerializer tkmsMessageSerializer() {
    return new TkmsMessageSerializer();
  }

  @Bean
  @ConditionalOnMissingBean(IEnvironmentValidator.class)
  public EnvironmentValidator tkmsMigrationHandler() {
    return new EnvironmentValidator();
  }

  @Bean
  @ConditionalOnMissingBean(TkmsClusterWideStateMonitor.class)
  public TkmsClusterWideStateMonitor tkmsClusterWideStateMonitor() {
    return new TkmsClusterWideStateMonitor();
  }

}
