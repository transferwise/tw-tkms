package com.transferwise.kafka.tkms.config;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.IProblemNotifier;
import com.transferwise.kafka.tkms.config.TkmsProperties.DatabaseDialect;
import com.transferwise.kafka.tkms.dao.ITkmsMessageSerializer;
import com.transferwise.kafka.tkms.dao.TkmsDao;
import com.transferwise.kafka.tkms.dao.TkmsMariaDao;
import com.transferwise.kafka.tkms.dao.TkmsPostgresDao;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;

public class TkmsDaoProvider implements ITkmsDaoProvider {

  @Autowired
  private TkmsProperties properties;
  @Autowired
  private ITkmsDataSourceProvider dataSourceProvider;
  @Autowired
  private ITkmsMetricsTemplate metricsTemplate;
  @Autowired
  private ITkmsMessageSerializer messageSerializer;
  @Autowired
  private ITransactionsHelper transactionsHelper;
  @Autowired
  private IProblemNotifier problemNotifier;

  private Map<Pair<DatabaseDialect, DataSource>, TkmsDao> tkmsDaos = new HashMap<>();

  @PostConstruct
  public void init() {
    for (int s = 0; s < properties.getShardsCount(); s++) {
      var dataSource = dataSourceProvider.getDataSource(s);
      var dialect = properties.getDatabaseDialect(s);

      var shardTkmsDao = tkmsDaos.computeIfAbsent(ImmutablePair.of(dialect, dataSource), k -> {
        var tkmsDao = createTkmsDao(dialect, dataSource);
        tkmsDao.validateDatabase();
        return tkmsDao;
      });
      shardTkmsDao.validateDatabase(s);
    }
  }

  @Override
  public TkmsDao getTkmsDao(int shard) {
    var dialect = properties.getDatabaseDialect(shard);
    var dataSource = dataSourceProvider.getDataSource(shard);

    return tkmsDaos.get(ImmutablePair.of(dialect, dataSource));
  }

  protected TkmsDao createTkmsDao(DatabaseDialect dialect, DataSource dataSource) {
    var dao = createTkmsDao0(dialect, dataSource);
    dao.init();
    
    return dao;
  }
  
  protected TkmsDao createTkmsDao0(DatabaseDialect dialect, DataSource dataSource) {
    if (dialect == DatabaseDialect.POSTGRES) {
      return new TkmsPostgresDao(dataSource, properties, metricsTemplate, messageSerializer, transactionsHelper, problemNotifier);
    }
    
    return new TkmsMariaDao(dataSource, properties, metricsTemplate, messageSerializer, transactionsHelper, problemNotifier);
  }
}
