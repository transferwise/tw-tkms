package com.transferwise.kafka.tkms.config;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.kafka.tkms.EnvironmentValidator;
import com.transferwise.kafka.tkms.IEnvironmentValidator;
import com.transferwise.kafka.tkms.IProblemNotifier;
import com.transferwise.kafka.tkms.ITkmsInterrupterService;
import com.transferwise.kafka.tkms.ITkmsMessagePollerFactory;
import com.transferwise.kafka.tkms.ITkmsPaceMaker;
import com.transferwise.kafka.tkms.ITkmsStorageToKafkaProxy;
import com.transferwise.kafka.tkms.ITkmsTopicValidator;
import com.transferwise.kafka.tkms.ITkmsZookeeperOperations;
import com.transferwise.kafka.tkms.JavaxValidationEnvironmentValidator;
import com.transferwise.kafka.tkms.ProblemNotifier;
import com.transferwise.kafka.tkms.TkmsInterrupterService;
import com.transferwise.kafka.tkms.TkmsMessageInterceptors;
import com.transferwise.kafka.tkms.TkmsMessagePollerFactory;
import com.transferwise.kafka.tkms.TkmsPaceMaker;
import com.transferwise.kafka.tkms.TkmsStorageToKafkaProxy;
import com.transferwise.kafka.tkms.TkmsTopicValidator;
import com.transferwise.kafka.tkms.TkmsZookeeperOperations;
import com.transferwise.kafka.tkms.TransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.ITkmsMessageInterceptors;
import com.transferwise.kafka.tkms.api.ITransactionalKafkaMessageSender;
import com.transferwise.kafka.tkms.api.Tkms;
import com.transferwise.kafka.tkms.api.helpers.ITkmsMessageFactory;
import com.transferwise.kafka.tkms.api.helpers.TkmsMessageFactory;
import com.transferwise.kafka.tkms.dao.ITkmsMessageSerializer;
import com.transferwise.kafka.tkms.dao.TkmsMessageSerializer;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.metrics.TkmsClusterWideStateMonitor;
import com.transferwise.kafka.tkms.metrics.TkmsMetricsTemplate;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Type;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

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
  public TkmsMetricsTemplate tkmsMetricsTemplate(IMeterCache meterCache, TkmsProperties tkmsProperties) {
    return new TkmsMetricsTemplate(meterCache, tkmsProperties);
  }

  @Bean
  @ConditionalOnBean(ITkmsMetricsTemplate.class)
  public MeterFilter tkmsMeterFilter() {
    Map<String, double[]> slos = new HashMap<>();
    double[] defaultSlos = new double[]{1, 5, 25, 125, 625, 3125, 15625};
    slos.put(TkmsMetricsTemplate.TIMER_PROXY_POLL, defaultSlos);
    slos.put(TkmsMetricsTemplate.TIMER_PROXY_CYCLE, defaultSlos);
    slos.put(TkmsMetricsTemplate.TIMER_PROXY_CYCLE_PAUSE, defaultSlos);
    slos.put(TkmsMetricsTemplate.TIMER_DAO_POLL_FIRST_RESULT, defaultSlos);
    slos.put(TkmsMetricsTemplate.TIMRE_DAO_POLL_ALL_RESULTS, defaultSlos);
    slos.put(TkmsMetricsTemplate.TIMER_DAO_POLL_GET_CONNECTION, defaultSlos);
    slos.put(TkmsMetricsTemplate.TIMER_PROXY_KAFKA_MESSAGES_SEND, defaultSlos);
    slos.put(TkmsMetricsTemplate.TIMER_PROXY_MESSAGES_DELETION, defaultSlos);
    slos.put(TkmsMetricsTemplate.SUMMARY_DAO_POLL_ALL_RESULTS_COUNT, defaultSlos);
    slos.put(TkmsMetricsTemplate.TIMER_MESSAGE_INSERT_TO_ACK, new double[]{1, 5, 25, 125, 625, 3125, 15625});
    slos.put(TkmsMetricsTemplate.SUMMARY_DAO_COMPRESSION_RATIO_ACHIEVED, new double[]{0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.25, 2, 4});
    slos.put(TkmsMetricsTemplate.SUMMARY_MESSAGES_IN_TRANSACTION, new double[]{1, 5, 25, 125, 625, 3125, 15625, 5 * 15625});

    return new MeterFilter() {
      @Override
      public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
        double[] sloConfigValues = slos.get(id.getName());
        if (sloConfigValues != null) {
          double[] sloValues = Arrays.copyOf(sloConfigValues, sloConfigValues.length);
          if (id.getType() == Type.TIMER) {
            for (int i = 0; i < sloValues.length; i++) {
              sloValues[i] = sloValues[i] * 1_000_000L;
            }
          }
          return DistributionStatisticConfig.builder()
              .percentilesHistogram(false)
              .serviceLevelObjectives(sloValues)
              .build()
              .merge(config);
        }
        return config;
      }
    };
  }

  @Bean
  @ConditionalOnMissingBean(ITransactionalKafkaMessageSender.class)
  public TransactionalKafkaMessageSender tkmsTransactionalKafkaMessageSender() {
    return new TransactionalKafkaMessageSender();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsDaoProvider.class)
  public TkmsDaoProvider tkmsDaoProvider() {
    return new TkmsDaoProvider();
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

  /**
   * This would work for simple services, mostly when having one database.
   *
   * <p>For more advanced cases it is recommended to define your own `ITkmsDataSourceProvider` implementation.
   */
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
  @ConditionalOnBean(type = "jakarta.validation.Validator")
  public EnvironmentValidator tkmsEnvironmentValidator() {
    return new EnvironmentValidator();
  }

  @Bean
  @ConditionalOnMissingBean(IEnvironmentValidator.class)
  @ConditionalOnBean(type = "javax.validation.Validator")
  public JavaxValidationEnvironmentValidator tkmsJavaxValidationEnvironmentValidator() {
    return new JavaxValidationEnvironmentValidator();
  }

  @Bean
  @ConditionalOnMissingBean(TkmsClusterWideStateMonitor.class)
  public TkmsClusterWideStateMonitor tkmsClusterWideStateMonitor() {
    return new TkmsClusterWideStateMonitor();
  }

  @Bean
  @ConditionalOnMissingBean(IProblemNotifier.class)
  public ProblemNotifier tkmsProblemNotifier() {
    return new ProblemNotifier();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsInterrupterService.class)
  public TkmsInterrupterService tkmsInterrupterService() {
    return new TkmsInterrupterService();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsKafkaAdminProvider.class)
  public ITkmsKafkaAdminProvider tkmsKafkaAdminProvider() {
    return new TkmsKafkaAdminProvider();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsTopicValidator.class)
  public ITkmsTopicValidator tkmsTopicValidator() {
    return new TkmsTopicValidator();
  }

  @Bean
  @ConditionalOnMissingBean(ITkmsMessagePollerFactory.class)
  public ITkmsMessagePollerFactory tkmsMessagePollerFactory() {
    return new TkmsMessagePollerFactory();
  }
}
