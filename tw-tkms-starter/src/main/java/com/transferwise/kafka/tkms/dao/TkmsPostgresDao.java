package com.transferwise.kafka.tkms.dao;

import com.google.common.primitives.Longs;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
public class TkmsPostgresDao extends TkmsDao {

  public static final Pattern N_DISTINCT_PATTERN = Pattern.compile("n_distinct=(.*)[,}]");

  @Override
  protected String getInsertSql(TkmsShardPartition shardPartition) {
    return "insert into " + getTableName(shardPartition) + " (message) values (?) returning id";
  }

  @Override
  protected String getSelectSql(TkmsShardPartition shardPartition) {
    return "select id, message from " + getTableName(shardPartition) + " where id >= ? order by id limit ?";
  }

  @Override
  protected boolean doesEarliestVisibleMessagesTableExist() {
    String schema = currentSchema;
    String table = properties.getEarliestVisibleMessages().getTableName();
    if (StringUtils.contains(table, ".")) {
      schema = StringUtils.substringBefore(table, ".");
      table = StringUtils.substringAfter(table, ".");
    }

    return !jdbcTemplate.queryForList("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", String.class,
        schema, table).isEmpty();
  }

  @Override
  protected String getCurrentSchema() {
    return jdbcTemplate.queryForObject("select current_schema()", String.class);
  }

  @Override
  protected void validateEngineSpecificSchema() {
    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(); p++) {
        TkmsShardPartition sp = TkmsShardPartition.of(s, p);

        long distinctIdsCount = getDistinctIdsCount(sp);

        if (distinctIdsCount < 100000) {
          log.warn("Table for " + sp + " is not properly configured. Rows from table stats is " + distinctIdsCount
              + ". This can greatly affect performance during peaks or database slownesses.");
        }

        metricsTemplate.registerRowsInTableStats(sp, distinctIdsCount);
      }
    }
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

  protected long getDistinctIdsCount(TkmsShardPartition sp) {
    List<String> relOptionsList =
        jdbcTemplate.queryForList("select attoptions from pg_attribute, pg_class, pg_namespace where pg_class.oid = pg_attribute.attrelid "
            + "and pg_class.relnamespace = pg_namespace.oid "
            + "and pg_namespace.nspname=? and pg_class.relname=? and attname='id'", String.class, getSchemaName(sp), getTableNameWithoutSchema(sp));

    if (relOptionsList.isEmpty()) {
      return -1;
    }

    String relOptions = relOptionsList.get(0);

    Matcher m = N_DISTINCT_PATTERN.matcher(relOptions);

    if (m.find()) {
      Long value = Longs.tryParse(relOptions.substring(m.start(1), m.end(1)));
      return value == null ? -1 : value;
    } else {
      return -1;
    }
  }
}
