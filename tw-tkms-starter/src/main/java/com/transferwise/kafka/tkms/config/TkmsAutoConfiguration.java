package com.transferwise.kafka.tkms.config;

import com.transferwise.common.baseutils.concurrency.DefaultExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.common.baseutils.meters.cache.MeterCache;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.common.baseutils.transactionsmanagement.TransactionsHelper;
import com.transferwise.kafka.tkms.api.ITkmsMessageDecorator;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import java.util.Collections;
import java.util.List;

@Configuration
@AutoConfigureAfter({FlywayAutoConfiguration.class, ValidationAutoConfiguration.class})
@Import(TkmsConfiguration.class)
@Slf4j
public class TkmsAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(IExecutorServicesProvider.class)
  public DefaultExecutorServicesProvider tkmsExecutorServicesProvider() {
    return new DefaultExecutorServicesProvider();
  }

  @Bean
  @ConditionalOnMissingBean(IMeterCache.class)
  public IMeterCache twDefaultMeterCache(MeterRegistry meterRegistry) {
    return new MeterCache(meterRegistry);
  }

  @Bean
  @ConditionalOnMissingBean(ITransactionsHelper.class)
  public TransactionsHelper twTransactionsHelper() {
    return new TransactionsHelper();
  }

  @Bean
  @ConditionalOnMissingBean
  public List<ITkmsMessageDecorator> messageDecorators() {
    return Collections.emptyList();
  }

}
