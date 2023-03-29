package com.transferwise.kafka.tkms.config;

import javax.sql.DataSource;

public interface ITkmsDataSourceProvider {

  /**
   * Provides the datasource for tw-tkms to use.
   */
  DataSource getDataSource(int shard);
}
