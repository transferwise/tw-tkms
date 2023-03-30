package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsDataSourceProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

public class TkmsTestDao implements ITkmsTestDao {

  @Autowired
  private TkmsProperties properties;

  @Autowired
  private ITkmsDataSourceProvider tkmsDataSourceProvider;

  @Override
  public int getMessagesCount(TkmsShardPartition shardPartition) {
    var dataSource = tkmsDataSourceProvider.getDataSource(shardPartition.getShard());
    var jdbcTemplate = new JdbcTemplate(dataSource);
    
    Integer messagesCount = jdbcTemplate.queryForObject("select count(*) from " + getTableName(shardPartition), Integer.class);
    if (messagesCount == null) {
      throw new IllegalStateException("Messages count can not be null.");
    }
    return messagesCount;
  }
  
  protected String getTableName(TkmsShardPartition shardPartition) {
    return properties.getTableBaseName(shardPartition.getShard()) + "_" + shardPartition.getShard() + "_" + shardPartition.getPartition();
  }
}
