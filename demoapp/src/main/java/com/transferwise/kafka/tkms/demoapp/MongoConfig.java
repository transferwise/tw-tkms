package com.transferwise.kafka.tkms.demoapp;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.WriteResultChecking;

@Configuration
@Profile("mongo")
public class MongoConfig {

  @Autowired
  private MongoTemplate mongoTemplate;

  @PostConstruct
  public void init() {
    mongoTemplate.setWriteConcern(WriteConcern.MAJORITY);
    mongoTemplate.setWriteConcernResolver(a -> WriteConcern.MAJORITY);
    mongoTemplate.setWriteResultChecking(WriteResultChecking.EXCEPTION);
    mongoTemplate.setReadPreference(ReadPreference.primary());
  }
}
