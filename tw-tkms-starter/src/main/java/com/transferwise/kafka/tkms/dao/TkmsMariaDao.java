package com.transferwise.kafka.tkms.dao;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsDataSourceProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.metrics.MonitoringQuery;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
public class TkmsMariaDao extends TkmsDao {

    public TkmsMariaDao(ITkmsDataSourceProvider dataSourceProvider, TkmsProperties properties, ITkmsMetricsTemplate metricsTemplate, ITkmsMessageSerializer messageSerializer, ITransactionsHelper transactionsHelper) {
        super(dataSourceProvider, properties, metricsTemplate, messageSerializer, transactionsHelper);
    }

    @Override
    @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
    @MonitoringQuery
    public long getApproximateMessagesCount(TkmsShardPartition sp) {
        List<Long> rows = jdbcTemplate.queryForList("select table_rows from information_schema.tables where table_schema=? and table_name = ?", Long.class, getSchemaName(sp), getTableNameWithoutSchema(sp));

        return rows.isEmpty() ? -1 : rows.get(0);
    }

    protected void validateEngineSpecificSchema() {
        if (!properties.isTableStatsValidationEnabled()) {
            return;
        }

        try {
            for (int s = 0; s < properties.getShardsCount(); s++) {
                for (int p = 0; p < properties.getPartitionsCount(s); p++) {
                    TkmsShardPartition sp = TkmsShardPartition.of(s, p);

                    long rowsInTableStats = getRowsFromTableStats(sp);
                    long rowsInIndexStats = getRowsFromIndexStats(sp);

                    if (rowsInTableStats < 100000 || rowsInIndexStats < 100000) {
                        log.warn("Table for " + sp + " is not properly configured. Rows from table stats is " + rowsInTableStats + ", rows in index stats is " + rowsInIndexStats + ". This can greatly affect performance during peaks or database slownesses.");
                    }

                    metricsTemplate.registerRowsInTableStats(sp, rowsInTableStats);
                    metricsTemplate.registerRowsInIndexStats(sp, rowsInIndexStats);
                }
            }
        } catch (Throwable t) {
            log.error("Validating table and index stats failed. Will still continue with the initialization.", t);
        }
    }

    private long getRowsFromTableStats(TkmsShardPartition shardPartition) {
        List<Long> stats = jdbcTemplate.queryForList("select n_rows from mysql.innodb_table_stats where database_name=? and table_name=?", Long.class, getSchemaName(shardPartition), getTableNameWithoutSchema(shardPartition));

        if (stats.isEmpty()) {
            return -1;
        }
        return stats.get(0);
    }

    private long getRowsFromIndexStats(TkmsShardPartition shardPartition) {
        List<Long> stats = jdbcTemplate.queryForList("select stat_value from mysql.innodb_index_stats where database_name=? and stat_description='id' and " + "table_name=?", Long.class, getSchemaName(shardPartition), getTableNameWithoutSchema(shardPartition));

        if (stats.isEmpty()) {
            return -1;
        }
        return stats.get(0);
    }

    protected boolean doesEarliestVisibleMessagesTableExist() {
        String schema = currentSchema;
        String table = properties.getEarliestVisibleMessages().getTableName();
        if (StringUtils.contains(table, ".")) {
            schema = StringUtils.substringBefore(table, ".");
            table = StringUtils.substringAfter(table, ".");
        }

        return !jdbcTemplate.queryForList("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", Boolean.class, schema, table).isEmpty();
    }

    protected String getInsertSql(TkmsShardPartition shardPartition) {
        return "insert into " + getTableName(shardPartition) + " (message) values (?)";
    }

    protected String getSelectSql(TkmsShardPartition shardPartition) {
        return "select id, message from " + getTableName(shardPartition) + " use index (PRIMARY) where id >= ? order by id limit ?";
    }

    protected String getCurrentSchema() {
        return jdbcTemplate.queryForObject("select DATABASE()", String.class);
    }
}
