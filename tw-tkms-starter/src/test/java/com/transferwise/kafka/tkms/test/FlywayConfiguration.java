package com.transferwise.kafka.tkms.test;

import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class FlywayConfiguration {

  @Bean
  @Primary
  public FlywayMigrationStrategy twTkmsFlywayMigrationStrategy() {
    return (flyway -> {
      flyway.clean();
      flyway.migrate();
    });
  }
}
