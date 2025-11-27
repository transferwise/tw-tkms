package com.transferwise.kafka.tkms.dao;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.IProblemNotifier;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationLevel;
import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationType;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import java.util.List;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
public class TkmsMariaDao extends TkmsDao {

  public TkmsMariaDao(
      DataSource dataSource,
      TkmsProperties properties,
      ITkmsMetricsTemplate metricsTemplate,
      ITkmsMessageSerializer messageSerializer,
      ITransactionsHelper transactionsHelper,
      IProblemNotifier problemNotifier
  ) {
    super(dataSource, properties, metricsTemplate, messageSerializer, transactionsHelper, problemNotifier);
  }

  @Override
  @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
  public long getApproximateMessagesCount(TkmsShardPartition sp) {
    List<Long> rows =
        jdbcTemplate.queryForList("select table_rows from information_schema.tables where table_schema=? and table_name = ?", Long.class,
            getSchemaName(sp), getTableNameWithoutSchema(sp));

    return rows.isEmpty() ? -1 : rows.get(0);
  }

  @Override
  public void validateDatabase() {
    transactionsHelper.withTransaction().withIsolation(Isolation.READ_UNCOMMITTED).run(() -> {
      super.validateDatabase();

      if (!properties.isTableStatsValidationEnabled()) {
        return;
      }

      try {
        boolean engineIndependentStatsEnabled = isEngineIndependentStatsEnabled();
        if (!engineIndependentStatsEnabled) {
          problemNotifier.notify(null, NotificationType.ENGINE_INDEPENDENT_STATS_NOT_ENABLED, NotificationLevel.WARN, () ->
              "Engine independent statistics are not enabled.");
        }
      } catch (DataAccessException dae) {
        problemNotifier.notify(null, NotificationType.TABLE_INDEX_STATS_CHECK_ERROR, NotificationLevel.ERROR, () ->
            "Checking if engine independent stats are enabled, failed.", dae);
      }
    });
  }

  @Override
  public void validateDatabase(int shard) {
    transactionsHelper.withTransaction().withIsolation(Isolation.READ_UNCOMMITTED).run(() -> {
      super.validateDatabase(shard);
      try {
        boolean engineIndependentStatsEnabled = isEngineIndependentStatsEnabled();

        for (int p = 0; p < properties.getPartitionsCount(shard); p++) {
          TkmsShardPartition sp = TkmsShardPartition.of(shard, p);

          if (!engineIndependentStatsEnabled) {
            long rowsInTableStats = getRowsFromTableStats(sp);
            metricsTemplate.registerRowsInTableStats(sp, rowsInTableStats);

            // Default log level should be at least error, because misconfiguration here can take down your database.
            if (rowsInTableStats < 1_000_000) {
              problemNotifier.notify(shard, NotificationType.TABLE_STATS_NOT_FIXED, NotificationLevel.ERROR, () ->
                  "Table for " + sp + " is not properly configured. Rows from table stats is " + rowsInTableStats + "."
                      + "This can greatly affect performance of DELETE queries during peaks or database slowness. Please check the setup guide how "
                      + "to fix table stats."
              );
            }

            long rowsInIndexStats = getRowsFromIndexStats(sp);
            metricsTemplate.registerRowsInIndexStats(sp, rowsInIndexStats);

            if (rowsInIndexStats < 1_000_000) {
              problemNotifier.notify(shard, NotificationType.INDEX_STATS_NOT_FIXED, NotificationLevel.ERROR, () ->
                  "Table for " + sp + " is not properly configured. Rows in index stats is " + rowsInIndexStats + "."
                      + " This can greatly affect performance of DELETE queries during peaks or database slowness. Please check the setup guide how "
                      + "to fix index stats."
              );
            }
          }

          long rowsInEngineIndependentTableStats = getRowsFromEngineIndependentTableStats(sp);
          metricsTemplate.registerRowsInEngineIndependentTableStats(sp, rowsInEngineIndependentTableStats);

          if (rowsInEngineIndependentTableStats < 1_000_000) {
            problemNotifier.notify(shard, NotificationType.ENGINE_INDEPENDENT_TABLE_STATS_NOT_FIXED, NotificationLevel.ERROR, () ->
                "Table for " + sp + " is not properly configured. Rows in engine independent table stats is " + rowsInEngineIndependentTableStats
                    + ". This can greatly affect performance of DELETE queries during peaks or database slowness. Please check the setup guide how "
                    + "to fix table stats."
            );
          }
        }
      } catch (DataAccessException dae) {
        // TODO: Currently our database may not have enough permissions yet, so starting with WARN.
        //       Later we should upgrade the default to ERROR.
        problemNotifier.notify(shard, NotificationType.TABLE_INDEX_STATS_CHECK_ERROR, NotificationLevel.WARN, () ->
            "Validating table and index stats failed.", dae);
      }

    });
  }

  protected boolean isEngineIndependentStatsEnabled() {
    var userStatTables = getUserStatTablesVariable();
    return userStatTables.equalsIgnoreCase("preferably") || userStatTables.equalsIgnoreCase("preferably_for_queries");
  }

  private long getRowsFromTableStats(TkmsShardPartition shardPartition) {
    List<Long> stats = jdbcTemplate.queryForList("select n_rows from mysql.innodb_table_stats where database_name=? and table_name=?", Long.class,
        getSchemaName(shardPartition), getTableNameWithoutSchema(shardPartition));

    if (stats.isEmpty()) {
      return -1L;
    }
    return stats.get(0);
  }

  private long getRowsFromEngineIndependentTableStats(TkmsShardPartition shardPartition) {
    List<Long> stats = jdbcTemplate.queryForList("select cardinality from mysql.table_stats where db_name=? and table_name=?", Long.class,
        getSchemaName(shardPartition), getTableNameWithoutSchema(shardPartition));

    if (stats.isEmpty()) {
      return -1L;
    }
    return stats.get(0);
  }

  private String getUserStatTablesVariable() {
    return jdbcTemplate.queryForObject("select @@use_stat_tables", String.class);
  }

  private long getRowsFromIndexStats(TkmsShardPartition shardPartition) {
    List<Long> stats = jdbcTemplate.queryForList(
        "select stat_value from mysql.innodb_index_stats where database_name=? and stat_description='id' and " + "table_name=?", Long.class,
        getSchemaName(shardPartition), getTableNameWithoutSchema(shardPartition));

    if (stats.isEmpty()) {
      return -1L;
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

    return !jdbcTemplate.queryForList("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", Boolean.class,
        schema, table).isEmpty();
  }

  @Override
  protected String getInsertSql(TkmsShardPartition shardPartition) {
    return "insert into " + getTableName(shardPartition) + " (message) values (?)";
  }

  @Override
  protected String getEarliestMessageIdSql(TkmsShardPartition shardPartition) {
    var earliestVisibleMessages = properties.getEarliestVisibleMessages(shardPartition.getShard());
    return "select message_id from " + earliestVisibleMessages.getTableName() + " use index(PRIMARY) where shard=? and part=?";
  }

  @Override
  public boolean hasMessagesBeforeId(TkmsShardPartition shardPartition, Long messageId) {
    return transactionsHelper.withTransaction().withIsolation(Isolation.READ_UNCOMMITTED)
        .call(() -> super.hasMessagesBeforeId(shardPartition, messageId));
  }

  @Override
  protected String getSelectSql(TkmsShardPartition shardPartition) {
    return "select id, message from " + getTableName(shardPartition) + " use index (PRIMARY) where id >= ? order by id limit ?";
  }

  @Override
  protected String getSelectWithOffsetSql(TkmsShardPartition shardPartition) {
    return "select id, message from " + getTableName(shardPartition) + " use index (PRIMARY) where id >= ? order by id limit ? offset ?";
  }

  @Override
  protected String getHasMessagesBeforeIdSql(TkmsShardPartition shardPartition) {
    return "select 1 from " + getTableName(shardPartition) + " use index(PRIMARY) where id < ? limit 1";
  }

  @Override
  protected String getExplainClause() {
    return "EXPLAIN FORMAT=JSON";
  }

  @Override
  protected boolean isUsingIndexScan(String explainPlan) {
    return explainPlan.contains("\"key\": \"PRIMARY\"");
  }

  @Override
  protected String getDeleteSql(TkmsShardPartition shardPartition, int batchSize) {
    // MariaDb does not support index hints for delete queries.
    // But MySQL does, so we will still include it in the query.
    var tableName = getTableName(shardPartition);
    var sb = new StringBuilder("delete /*+ INDEX(" + tableName + ") */ from " + tableName + " where id in (");
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
