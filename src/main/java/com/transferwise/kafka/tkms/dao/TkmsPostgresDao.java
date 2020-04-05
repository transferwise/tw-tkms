package com.transferwise.kafka.tkms.dao;

import com.transferwise.kafka.tkms.dao.TkmsDao;

public class TkmsPostgresDao extends TkmsDao {

  @Override
  protected String getInsertSql(int shard) {
    return "insert into " + properties.getTableBaseName() + "_" + shard + " (topic, message_key, timestamp, message) values (?,?,?,?) returning id";
  }
}
