package com.transferwise.kafka.tkms.config;

import javax.sql.DataSource;

public class TkmsDataSourceProvider implements ITkmsDataSourceProvider {

  private final DataSource dataSource;

  public TkmsDataSourceProvider(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public DataSource getDataSource(int shard) {
    return dataSource;
  }
}
