package com.transferwise.kafka.tkms.config;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.WriteResultChecking;

@Configuration
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
