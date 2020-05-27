package com.transferwise.kafka.tkms.dao;

import com.transferwise.kafka.tkms.ShardPartition;

public class TkmsPostgresDao extends TkmsDao {

  @Override
  protected String getInsertSql(ShardPartition shardPartition) {
    return "insert into " + getTableName(shardPartition) + " (message) values (?) returning id";
  }
}
