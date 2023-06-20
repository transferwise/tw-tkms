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

@Slf4j
public class TkmsPostgresDao extends TkmsDao {

  public TkmsPostgresDao(
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
  protected String getInsertSql(TkmsShardPartition shardPartition) {
    return "insert into " + getTableName(shardPartition) + " (message) values (?) returning id";
  }

  @Override
  protected String getSelectSql(TkmsShardPartition shardPartition) {
    return "select /*+ IndexScan(om) */ id, message from " + getTableName(shardPartition) + " om where id >= ? order by id limit ?";
  }

  @Override
  protected String getHasMessagesBeforeIdSql(TkmsShardPartition shardPartition) {
    return "select /*+ IndexOnlyScan(om)  */ 1 from " + getTableName(shardPartition) + " om where id < ? order by id desc limit 1";
  }

  @Override
  protected String getExplainClause() {
    return "EXPLAIN";
  }

  @Override
  protected boolean isUsingIndexScan(String explainPlan) {
    return explainPlan.contains("Index Scan using") || explainPlan.contains("Index Only Scan Backward using")
        || explainPlan.contains("Index Only Scan using") || explainPlan.contains("Bitmap Heap Scan");
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

    return !jdbcTemplate.queryForList("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", String.class,
        schema, table).isEmpty();
  }

  @Override
  protected String getCurrentSchema() {
    return jdbcTemplate.queryForObject("select current_schema()", String.class);
  }

  @Override
  public void validateDatabase() {
    // TODO: Should this be shard specific as well?
    if (!properties.isTableStatsValidationEnabled()) {
      return;
    }

    super.validateDatabase();

    validateIndexHintsExtension();
  }

  protected boolean validateIndexHintsExtension() {
    var seqScans = doesRespectHint("SeqScan(om)", "Seq Scan");
    var indexOnlyScans = doesRespectHint("IndexOnlyScan(om)", "Index Only Scan", "Bitmap Index Scan");

    if (!seqScans || !indexOnlyScans) {
      // By default, logged as ERROR, as it can cause pretty serious problems.
      problemNotifier.notify(null, NotificationType.INDEX_HINTS_NOT_AVAILABLE, NotificationLevel.ERROR,
          () -> "Query hints are not supported. This can greatly affect performance during peaks or database slowness. Make sure `pg_hint_plan` "
              + "extension is available.");

      return false;
    }

    return true;
  }

  protected boolean doesRespectHint(String hint, String... expectedPlans) {
    var table = getTableName(TkmsShardPartition.of(0, 0));
    var explainResult = getExplainResult("select /*+ " + hint + " */ id from " + table + " om where id = 1");
    for (var expectedPlan : expectedPlans) {
      if (explainResult.contains(expectedPlan)) {
        return true;
      }
    }
    log.info("When checking index hint '{}', the explain plan was '{}', and it did not contain '{}'.", hint, explainResult,
        StringUtils.join(expectedPlans, "', '"));
    return false;
  }

  protected String getExplainResult(String sql) {
    var rows = jdbcTemplate.queryForList("EXPLAIN " + sql, String.class);
    return concatStringRows(rows);
  }

  @Override
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

}
