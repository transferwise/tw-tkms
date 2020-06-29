package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.ShardPartition;
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

  private JdbcTemplate jdbcTemplate;

  @PostConstruct
  public void init() {
    jdbcTemplate = new JdbcTemplate(tkmsDataSourceProvider.getDataSource());
  }

  @Override
  public int getMessagesCount(ShardPartition shardPartition) {
    Integer messagesCount = jdbcTemplate.queryForObject("select count(*) from " + getTableName(shardPartition), Integer.class);
    if (messagesCount == null) {
      throw new IllegalStateException("Messages count can not be null.");
    }
    return messagesCount;
  }


  protected String getTableName(ShardPartition shardPartition) {
    return properties.getTableBaseName() + "_" + shardPartition.getShard() + "_" + shardPartition.getPartition();
  }
}
