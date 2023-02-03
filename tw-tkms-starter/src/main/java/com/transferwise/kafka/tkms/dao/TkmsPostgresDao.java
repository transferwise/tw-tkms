package com.transferwise.kafka.tkms.dao;

import com.google.common.primitives.Longs;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.IProblemNotifier;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsDataSourceProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationLevel;
import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationType;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.metrics.MonitoringQuery;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
public class TkmsPostgresDao extends TkmsDao {

  private final IProblemNotifier problemNotifier;

  public TkmsPostgresDao(
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

  public static final Pattern N_DISTINCT_PATTERN = Pattern.compile("n_distinct=(.*)[,}]");

  @Override
  protected String getInsertSql(TkmsShardPartition shardPartition) {
    return "insert into " + getTableName(shardPartition) + " (message) values (?) returning id";
  }

  @Override
  protected String getSelectSql(TkmsShardPartition shardPartition) {
    return "select /*+ IndexScan(om) */ id, message from " + getTableName(shardPartition) + " om where id >= ? order by id limit ?";
  }

  @Override
  protected String getHasMessagesBeforeIdSql(TkmsShardPartition shardPartition) {
    return "select /*+ IndexOnlyScan(om) */ 1 from " + getTableName(shardPartition) + " om where id < ? order by id desc limit 1";
  }

  @Override
  protected String getExplainClause() {
    return "EXPLAIN";
  }

  @Override
  protected boolean isUsingIndexScan(String sql) {
    return sql.contains("Index Scan using");
  }

  @Override
  protected String getDeleteSql(TkmsShardPartition shardPartition, int batchSize) {
    var sb = new StringBuilder("delete /*+ IndexScan(om) */ from " + getTableName(shardPartition) + " om where id in (");
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
  protected boolean doesEarliestVisibleMessagesTableExist() {
    var defaultTable = properties.getEarliestVisibleMessages().getTableName();

    String schema;
    String table;
    if (StringUtils.contains(defaultTable, ".")) {
      schema = StringUtils.substringBefore(defaultTable, ".");
      table = StringUtils.substringAfter(defaultTable, ".");
    } else {
      schema = currentSchema;
      table = defaultTable;
    }

    return transactionsHelper.withTransaction().asNew().withIsolation(Isolation.READ_UNCOMMITTED).call(() ->
        !jdbcTemplate.queryForList("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", String.class,
            schema, table).isEmpty()
    );
  }

  @Override
  protected String getCurrentSchema() {
    return transactionsHelper.withTransaction().asNew().withIsolation(Isolation.READ_UNCOMMITTED).call(() ->
        jdbcTemplate.queryForObject("select current_schema()", String.class)
    );
  }

  @Override
  protected void validateEngineSpecifics() {
    if (!properties.isTableStatsValidationEnabled()) {
      return;
    }

    var indexHintsEnabled = validateIndexHints();

    for (int s = 0; s < properties.getShardsCount(); s++) {
      try {
        for (int p = 0; p < properties.getPartitionsCount(s); p++) {
          TkmsShardPartition sp = TkmsShardPartition.of(s, p);

          long distinctIdsCount = getDistinctIdsCount(sp);
          if (!indexHintsEnabled && distinctIdsCount < 1_000_000) {
            problemNotifier.notify(s, NotificationType.TABLE_STATS_NOT_FIXED, NotificationLevel.ERROR, () ->
                "Table for " + sp + " is not properly configured. n_distinct is just set to " + distinctIdsCount + "."
                    + " This can greatly affect performance of DELETE queries during peaks or database slowness. Please check the setup guide how "
                    + "to fix table stats."
            );
          }

          metricsTemplate.registerRowsInTableStats(sp, distinctIdsCount);
        }
      } catch (DataAccessException dae) {
        problemNotifier.notify(s, NotificationType.TABLE_INDEX_STATS_CHECK_ERROR, NotificationLevel.ERROR, () ->
            "Validating table and index stats failed.", dae);
      }
    }
  }

  protected boolean validateIndexHints() {
    var seqScans = doesRespectHint("SeqScan", "Seq Scan");
    var indexOnlyScans = doesRespectHint("IndexOnlyScan", "Index Only Scan");

    if (!seqScans || !indexOnlyScans) {
      // By default, logged as ERROR, as it can cause pretty serious problems.
      problemNotifier.notify(null, NotificationType.INDEX_HINTS_NOT_AVAILABLE, NotificationLevel.ERROR,
          () -> "Query hints are not supported. This can greatly affect performance during peaks or database slowness. Make sure `pg_hint_plan` "
              + "extension is available.");

      return false;
    }

    return true;
  }

  protected boolean doesRespectHint(String hint, String expectedPlan) {
    var table = getTableName(TkmsShardPartition.of(0, 0));
    var explainResult = getExplainResult("select /*+ " + hint + "(om) */ id from " + table + " om where id = 1");
    return explainResult.contains(expectedPlan);
  }

  protected String getExplainResult(String sql) {
    var rows = jdbcTemplate.queryForList("EXPLAIN " + sql, String.class);
    return concatStringRows(rows);
  }

  @Override
  @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
  public long getApproximateMessagesCount(TkmsShardPartition sp) {
    List<Long> rows =
        jdbcTemplate.queryForList("SELECT reltuples as approximate_row_count FROM pg_class, pg_namespace WHERE "
                + " pg_class.relnamespace=pg_namespace.oid and nspname=? and relname = ?", Long.class,
            getSchemaName(sp), getTableNameWithoutSchema(sp));

    return rows.isEmpty() ? -1 : rows.get(0);
  }

  @Override
  protected String getEarliestMessageIdSql(TkmsShardPartition shardPartition) {
    var earliestVisibleMessages = properties.getEarliestVisibleMessages(shardPartition.getShard());
    return "select /*+ IndexOnlyScan(om) */ message_id from " + earliestVisibleMessages.getTableName() + " om where shard=? and part=?";
  }

  protected long getDistinctIdsCount(TkmsShardPartition sp) {
    List<String> relOptionsList = transactionsHelper.withTransaction().asNew().withIsolation(Isolation.READ_UNCOMMITTED).call(() ->
        jdbcTemplate.queryForList("select attoptions from pg_attribute, pg_class, pg_namespace where pg_class.oid = pg_attribute.attrelid "
            + "and pg_class.relnamespace = pg_namespace.oid "
            + "and pg_namespace.nspname=? and pg_class.relname=? and attname='id'", String.class, getSchemaName(sp), getTableNameWithoutSchema(sp))
    );

    if (relOptionsList.isEmpty()) {
      return -1;
    }

    String relOptions = relOptionsList.get(0);
    if (relOptions == null) {
      return -1;
    }

    Matcher m = N_DISTINCT_PATTERN.matcher(relOptions);

    if (m.find()) {
      Long value = Longs.tryParse(relOptions.substring(m.start(1), m.end(1)));
      return value == null ? -1 : value;
    } else {
      return -1;
    }
  }

}
