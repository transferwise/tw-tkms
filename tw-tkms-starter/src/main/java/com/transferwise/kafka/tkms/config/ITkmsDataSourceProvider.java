package com.transferwise.kafka.tkms.config;

import javax.sql.DataSource;

public interface ITkmsDataSourceProvider {

  DataSource getDataSource();
}
