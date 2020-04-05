package com.transferwise.kafka.tkms.dao;

public class TkmsPostgresDao extends TkmsDao {

  @Override
  protected String getInsertSql(int shard) {
    return "insert into " + properties.getTableBaseName() + "_" + shard + " (message) values (?) returning id";
  }
}
