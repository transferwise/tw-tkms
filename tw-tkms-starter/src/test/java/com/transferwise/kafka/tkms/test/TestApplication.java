package com.transferwise.kafka.tkms.test;

import com.transferwise.common.baseutils.transactionsmanagement.TransactionsConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(TransactionsConfiguration.class)
@Slf4j
public class TestApplication implements InitializingBean {

  @Override
  public void afterPropertiesSet() {
    log.info("Starting Test Application.");
  }

  @Bean
  public TestMessagesInterceptor testMessagesIntereceptor() {
    return new TestMessagesInterceptor();
  }

  @Bean
  public FlywayMigrationStrategy flywayMigrationStrategy() {
    return flyway -> {
      // We clean the test database, so we don't have to remove the db container every time.
      flyway.clean();
      flyway.migrate();
    };
  }
}
