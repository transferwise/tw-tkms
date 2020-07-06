package com.transferwise.kafka.tkms.dao;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;

public class TkmsPostgresDao extends TkmsDao {

  @Override
  protected String getInsertSql(TkmsShardPartition shardPartition) {
    return "insert into " + getTableName(shardPartition) + " (message) values (?) returning id";
  }
}
