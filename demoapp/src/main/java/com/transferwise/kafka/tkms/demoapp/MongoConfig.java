package com.transferwise.kafka.tkms.demoapp;

import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.SpringDataMongoV3Driver;
import com.github.cloudyrock.spring.v5.MongockSpring5;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.WriteResultChecking;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

@Configuration
@Profile("mongo")
public class MongoConfig extends AbstractMongoClientConfiguration {

  @Autowired
  private MongoDbProperties properties;

  @Override
  protected String getDatabaseName() {
    return properties.getDbName();
  }

  @Override
  protected void configureClientSettings(MongoClientSettings.Builder builder) {
    builder
        .retryWrites(true)
        .retryReads(true)
        .writeConcern(WriteConcern.MAJORITY)
        .readPreference(ReadPreference.primary())
        .readConcern(ReadConcern.MAJORITY)
        .credential(MongoCredential.createCredential(properties.getRwUser(),
            properties.getAuthDbName(), properties.getRwUserPassword()))
        .applyToConnectionPoolSettings(pool -> pool
            .applySettings(properties.getConnectionPoolSettings())
        )
        .applyToSocketSettings(socket -> socket.applySettings(properties.getSocketSettings()))
        .applyToClusterSettings(cluster -> cluster
            .hosts(properties.getServerAddresses())
            .serverSelectionTimeout(properties.getConnectTimeoutSeconds(), TimeUnit.SECONDS)
        );
  }

  @Override
  public MongoTemplate mongoTemplate(MongoDatabaseFactory databaseFactory, MappingMongoConverter converter) {
    MongoTemplate mongoTemplate = super.mongoTemplate(databaseFactory, converter);
    mongoTemplate.setWriteConcern(WriteConcern.MAJORITY);
    mongoTemplate.setWriteConcernResolver(a -> WriteConcern.MAJORITY);
    mongoTemplate.setWriteResultChecking(WriteResultChecking.EXCEPTION);
    mongoTemplate.setReadPreference(ReadPreference.primary());
    return mongoTemplate;
  }

  @Bean
  public MongoTransactionManager transactionManager(MongoDatabaseFactory mongoDatabaseFactory) {
    return new MongoTransactionManager(mongoDatabaseFactory);
  }

  @Bean
  public MongockSpring5.MongockApplicationRunner mongockApplicationRunner(
      ApplicationContext springContext,
      MongoTemplate mongoTemplate) {
    return MongockSpring5.builder()
        .setDriver(SpringDataMongoV3Driver.withDefaultLock(mongoTemplate))
        .addChangeLogsScanPackage("db.migration.mongo")
        .setSpringContext(springContext)
        .buildApplicationRunner();
  }

  @Bean
  public SpringDataMongoV3Driver mongockApplicationDriver(MongoTemplate mongoTemplate) {
    SpringDataMongoV3Driver driver = SpringDataMongoV3Driver.withDefaultLock(mongoTemplate);
    // TODO add proper timeouts after testing
    //or .withLockSetting(mongoTemplate, acquiredForMinutes, maxWaitingFor, maxTries);
    return driver;
  }
}
