package com.transferwise.kafka.tkms.dao;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.IProblemNotifier;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsDataSourceProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationLevel;
import com.transferwise.kafka.tkms.config.TkmsProperties.Notifications;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.metrics.MonitoringQuery;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
public class TkmsMariaDao extends TkmsDao {

  private final IProblemNotifier problemNotifier;

  public TkmsMariaDao(
      ITkmsDataSourceProvider dataSourceProvider,
      TkmsProperties properties,
      ITkmsMetricsTemplate metricsTemplate,
      ITkmsMessageSerializer messageSerializer,
      ITransactionsHelper transactionsHelper,
      IProblemNotifier problemNotifier
  ) {
    super(dataSourceProvider, properties, metricsTemplate, messageSerializer, transactionsHelper);
    this.problemNotifier = problemNotifier;
  }

  @Override
  @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
  @MonitoringQuery
  public long getApproximateMessagesCount(TkmsShardPartition sp) {
    List<Long> rows =
        jdbcTemplate.queryForList("select table_rows from information_schema.tables where table_schema=? and table_name = ?", Long.class,
            getSchemaName(sp), getTableNameWithoutSchema(sp));

    return rows.isEmpty() ? -1 : rows.get(0);
  }

  @Override
  protected void validateEngineSpecifics() {
    if (!properties.isTableStatsValidationEnabled()) {
      return;
    }

    try {
      for (int s = 0; s < properties.getShardsCount(); s++) {
        for (int p = 0; p < properties.getPartitionsCount(s); p++) {
          TkmsShardPartition sp = TkmsShardPartition.of(s, p);

          long rowsInTableStats = getRowsFromTableStats(sp);
          metricsTemplate.registerRowsInTableStats(sp, rowsInTableStats);

          // Default log level should be at least error, because misconfiguration here can take down your database.
          if (rowsInTableStats < 1000000) {
            problemNotifier.notify(Notifications.TABLE_STATS_NOT_FIXED, NotificationLevel.ERROR, () ->
                "Table for " + sp + " is not properly configured. Rows from table stats is " + rowsInTableStats + "."
                    + "This can greatly affect performance of DELETE queries during peaks or database slowness. Please check the setup guide how "
                    + "to fix table stats."
            );
          }

          long rowsInIndexStats = getRowsFromIndexStats(sp);
          metricsTemplate.registerRowsInIndexStats(sp, rowsInIndexStats);

          if (rowsInIndexStats < 1000000) {
            problemNotifier.notify(Notifications.INDEX_STATS_NOT_FIXED, NotificationLevel.ERROR, () ->
                "Table for " + sp + " is not properly configured. Rows in index stats is " + rowsInIndexStats + "."
                    + " This can greatly affect performance of DELETE queries during peaks or database slowness. Please check the setup guide how "
                    + "to fix index stats."
            );
          }
        }
      }
    } catch (DataAccessException dae) {
      problemNotifier.notify(Notifications.TABLE_INDEX_STATS_CHECK_ERROR, NotificationLevel.BLOCK, () ->
          "Validating table and index stats failed. Will still continue with the initialization.", dae);
    }
  }

  private long getRowsFromTableStats(TkmsShardPartition shardPartition) {
    List<Long> stats = jdbcTemplate.queryForList("select n_rows from mysql.innodb_table_stats where database_name=? and table_name=?", Long.class,
        getSchemaName(shardPartition), getTableNameWithoutSchema(shardPartition));

    if (stats.isEmpty()) {
      return -1;
    }
    return stats.get(0);
  }

  private long getRowsFromIndexStats(TkmsShardPartition shardPartition) {
    List<Long> stats = jdbcTemplate.queryForList(
        "select stat_value from mysql.innodb_index_stats where database_name=? and stat_description='id' and " + "table_name=?", Long.class,
        getSchemaName(shardPartition), getTableNameWithoutSchema(shardPartition));

    if (stats.isEmpty()) {
      return -1;
    }
    return stats.get(0);
  }

  @Override
  protected boolean doesEarliestVisibleMessagesTableExist() {
    String schema;
    String table;
    var defaultTable = properties.getEarliestVisibleMessages().getTableName();
    if (StringUtils.contains(defaultTable, ".")) {
      schema = StringUtils.substringBefore(defaultTable, ".");
      table = StringUtils.substringAfter(defaultTable, ".");
    } else {
      schema = currentSchema;
      table = defaultTable;
    }

    return transactionsHelper.withTransaction().asNew().withIsolation(Isolation.READ_UNCOMMITTED).call(
        () -> !jdbcTemplate.queryForList("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", Boolean.class,
            schema, table).isEmpty()
    );
  }

  @Override
  protected String getInsertSql(TkmsShardPartition shardPartition) {
    return "insert into " + getTableName(shardPartition) + " (message) values (?)";
  }

  @Override
  protected String getSelectSql(TkmsShardPartition shardPartition) {
    return "select id, message from " + getTableName(shardPartition) + " use index (PRIMARY) where id >= ? order by id limit ?";
  }

  @Override
  protected String getDeleteSql(TkmsShardPartition shardPartition, int batchSize) {
    // MariaDb does not support index hints for delete queries.
    // But MySQL does, so we will still include it in the query.
    var sb = new StringBuilder("delete /*+ INDEX(outgoing_message_0_0) */ from " + getTableName(shardPartition) + " where id in (");
    for (int j = 0; j < batchSize; j++) {
      if (j > 0) {
        sb.append(",");
      }
      sb.append("?");
    }
    sb.append(")");

    return sb.toString();
  }

  @Override
  protected String getCurrentSchema() {
    return transactionsHelper.withTransaction().withIsolation(Isolation.READ_UNCOMMITTED).call(() ->
        jdbcTemplate.queryForObject("select DATABASE()", String.class)
    );
  }
}
