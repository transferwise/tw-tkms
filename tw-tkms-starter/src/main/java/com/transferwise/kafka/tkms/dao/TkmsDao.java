package com.transferwise.kafka.tkms.dao;

import com.google.common.collect.ImmutableMap;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.TkmsMessageWithSequence;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.ITkmsDataSourceProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.metrics.MonitoringQuery;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * READ_UNCOMMITTED isolation level is to speed up queries against information schema in databases with high number of tenants. E.g. Custom
 * environments.
 */
@Slf4j
@RequiredArgsConstructor
public abstract class TkmsDao implements ITkmsDao {

  private static final int[] batchSizes = {1024, 256, 64, 16, 4, 1};

  private Map<TkmsShardPartition, String> insertMessageSqls;

  private Map<TkmsShardPartition, String> getMessagesSqls;

  private Map<Pair<TkmsShardPartition, Integer>, String> deleteSqlsMap;

  @Autowired
  private final ITkmsDataSourceProvider dataSourceProvider;

  @Autowired
  protected final TkmsProperties properties;

  @Autowired
  protected final ITkmsMetricsTemplate metricsTemplate;

  @Autowired
  private final ITkmsMessageSerializer messageSerializer;

  @Autowired
  private final ITransactionsHelper transactionsHelper;

  protected JdbcTemplate jdbcTemplate;

  protected String currentSchema;

  @PostConstruct
  protected void init() {
    jdbcTemplate = new JdbcTemplate(dataSourceProvider.getDataSource());

    transactionsHelper.withTransaction().withIsolation(Isolation.READ_UNCOMMITTED).run(() -> currentSchema = getCurrentSchema());

    createInsertMessagesSqls();
    createGetMessagesSqls();
    createDeleteMessagesSqls();

    transactionsHelper.withTransaction().withIsolation(Isolation.READ_UNCOMMITTED).run(() -> {
      validateSchema();
      validateEngineSpecificSchema();
    });
  }

  protected void createInsertMessagesSqls() {
    Map<TkmsShardPartition, String> map = new HashMap<>();
    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(s); p++) {
        TkmsShardPartition sp = TkmsShardPartition.of(s, p);
        map.put(sp, getInsertSql(sp));
      }
    }
    insertMessageSqls = ImmutableMap.copyOf(map);
  }

  protected void createGetMessagesSqls() {
    Map<TkmsShardPartition, String> map = new HashMap<>();
    map.clear();
    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(s); p++) {
        TkmsShardPartition sp = TkmsShardPartition.of(s, p);
        map.put(sp, getSelectSql(sp));
      }
    }
    getMessagesSqls = ImmutableMap.copyOf(map);
  }

  protected void createDeleteMessagesSqls() {
    deleteSqlsMap = new HashMap<>();

    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(s); p++) {
        TkmsShardPartition sp = TkmsShardPartition.of(s, p);
        for (int batchSize : batchSizes) {
          Pair<TkmsShardPartition, Integer> key = ImmutablePair.of(sp, batchSize);
          StringBuilder sb = new StringBuilder("delete from " + getTableName(sp) + " where id in (");
          for (int j = 0; j < batchSize; j++) {
            sb.append("?");
            if (j < batchSize - 1) {
              sb.append(",");
            }
          }
          deleteSqlsMap.put(key, sb.append(")").toString());
        }
      }
    }
    deleteSqlsMap = ImmutableMap.copyOf(deleteSqlsMap);
  }

  protected void validateSchema() {
    if (!doesEarliestVisibleMessagesTableExist()) {
      for (int s = 0; s < properties.getShardsCount(); s++) {
        var earliestVisibleMessages = properties.getEarliestVisibleMessages(s);
        if (earliestVisibleMessages.isEnabled()) {
          throw new IllegalStateException("Earliest visible messages table does not exist for shard " + s + ".");
        }
      }
    } else {
      for (int s = 0; s < properties.getShardsCount(); s++) {
        var earliestVisibleMessages = properties.getEarliestVisibleMessages(s);
        if (earliestVisibleMessages.isEnabled()) {
          int partitions = properties.getPartitionsCount(s);
          for (int p = 0; p < partitions; p++) {
            TkmsShardPartition shardPartition = TkmsShardPartition.of(s, p);
            if (getEarliestMessageId(shardPartition) == null) {
              insertEarliestMessageId(shardPartition);
            }
          }
        }
      }
    }
  }

  protected abstract void validateEngineSpecificSchema();

  @Transactional(rollbackFor = Exception.class)
  @Override
  public List<InsertMessageResult> insertMessages(TkmsShardPartition shardPartition, List<TkmsMessageWithSequence> tkmsMessages) {
    return ExceptionUtils.doUnchecked(() -> {

      List<InsertMessageResult> results = new ArrayList<>();
      MutableInt idx = new MutableInt();
      while (idx.getValue() < tkmsMessages.size()) {
        Connection con = DataSourceUtils.getConnection(dataSourceProvider.getDataSource());
        try {
          PreparedStatement ps = con.prepareStatement(insertMessageSqls.get(shardPartition), Statement.RETURN_GENERATED_KEYS);
          try {
            int batchSize = Math.min(properties.getInsertBatchSize(shardPartition.getShard()), tkmsMessages.size() - idx.intValue());

            for (int i = 0; i < batchSize; i++) {
              TkmsMessageWithSequence tkmsMessageWithSequence = tkmsMessages.get(idx.intValue() + i);
              ps.setBinaryStream(1, serializeMessage(shardPartition, tkmsMessageWithSequence.getTkmsMessage()));

              ps.addBatch();

              results.add(new InsertMessageResult().setSequence(tkmsMessageWithSequence.getSequence()));
            }

            ps.executeBatch();

            ResultSet rs = ps.getGeneratedKeys();
            try {
              int i = 0;
              while (rs.next()) {
                Long id = rs.getLong(1);
                TkmsMessageWithSequence tkmsMessageWithSequence = tkmsMessages.get(idx.intValue() + i);
                InsertMessageResult insertMessageResult = results.get(idx.intValue() + i);
                insertMessageResult.setStorageId(id);
                metricsTemplate.recordDaoMessageInsert(shardPartition, tkmsMessageWithSequence.getTkmsMessage().getTopic());
                i++;
              }
              if (i != batchSize) {
                log.info("Invalid generated keys count: batchSize was " + batchSize + " but we received " + i + " keys.");
                metricsTemplate.recordDaoInvalidGeneratedKeysCount(shardPartition);
              }
            } finally {
              rs.close();
            }
            idx.add(batchSize);
          } finally {
            ps.close();
          }
        } finally {
          DataSourceUtils.releaseConnection(con, dataSourceProvider.getDataSource());
        }
      }
      return results;
    });
  }

  @Transactional(rollbackFor = Exception.class)
  @Override
  public InsertMessageResult insertMessage(TkmsShardPartition shardPartition, TkmsMessage message) {
    final InsertMessageResult result = new InsertMessageResult().setShardPartition(shardPartition);

    final KeyHolder keyHolder = new GeneratedKeyHolder();
    jdbcTemplate.update(con -> {
      PreparedStatement ps = con.prepareStatement(insertMessageSqls.get(shardPartition), Statement.RETURN_GENERATED_KEYS);
      try {
        ps.setBinaryStream(1, serializeMessage(shardPartition, message));
        return ps;
      } catch (Exception e) {
        ps.close();
        throw e;
      }
    }, keyHolder);

    metricsTemplate.recordDaoMessageInsert(shardPartition, message.getTopic());

    result.setStorageId(keyToLong(keyHolder));
    return result;
  }

  protected InputStream serializeMessage(TkmsShardPartition shardPartition, TkmsMessage message) {
    return ExceptionUtils.doUnchecked(() -> messageSerializer.serialize(shardPartition, message));
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
  protected long keyToLong(KeyHolder keyHolder) {
    return keyHolder.getKey().longValue();
  }

  @Override
  public List<MessageRecord> getMessages(TkmsShardPartition shardPartition, long earliestMessageId, int maxCount) {
    return ExceptionUtils.doUnchecked(() -> {
      long startNanoTime = System.nanoTime();

      Connection con = DataSourceUtils.getConnection(dataSourceProvider.getDataSource());
      try {
        metricsTemplate.recordDaoPollGetConnection(shardPartition, startNanoTime);
        startNanoTime = System.nanoTime();
        int i = 0;

        try (PreparedStatement ps = con.prepareStatement(getMessagesSqls.get(shardPartition))) {
          ps.setLong(1, earliestMessageId);
          ps.setLong(2, maxCount);

          List<MessageRecord> records = new ArrayList<>();
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              if (i++ == 0) {
                metricsTemplate.recordDaoPollFirstResult(shardPartition, startNanoTime);
              }

              MessageRecord messageRecord = new MessageRecord();
              messageRecord.setId(rs.getLong(1));
              messageRecord.setMessage(messageSerializer.deserialize(shardPartition, rs.getBinaryStream(2)));

              records.add(messageRecord);
            }
          }

          return records;
        } finally {
          metricsTemplate.recordDaoPollAllResults(shardPartition, i, startNanoTime);
        }
      } finally {
        DataSourceUtils.releaseConnection(con, dataSourceProvider.getDataSource());
      }
    });
  }

  @Override
  public void deleteMessages(TkmsShardPartition shardPartition, List<Long> ids) {
    if (deleteSqlsMap.containsKey(Pair.of(shardPartition, ids.size()))) {
      // There will be one query only, no need for explicit transaction.
      deleteMessages0(shardPartition, ids);
    } else {
      transactionsHelper.withTransaction().call(() -> {
        deleteMessages0(shardPartition, ids);
        return null;
      });
    }
  }

  @Override
  public Long getEarliestMessageId(TkmsShardPartition shardPartition) {
    var earliestVisibleMessages = properties.getEarliestVisibleMessages(shardPartition.getShard());
    var ids =
        jdbcTemplate.queryForList("select message_id from " + earliestVisibleMessages.getTableName() + " where shard=? and part=?", Long.class,
            shardPartition.getShard(), shardPartition.getPartition());
    return ids.isEmpty() ? null : ids.get(0);
  }

  @Override
  public void saveEarliestMessageId(TkmsShardPartition shardPartition, long messageId) {
    var earliestVisibleMessages = properties.getEarliestVisibleMessages(shardPartition.getShard());

    if (jdbcTemplate
        .update("update " + earliestVisibleMessages.getTableName() + " set message_id=? where shard=? and part=?", messageId,
            shardPartition.getShard(), shardPartition.getPartition()) != 1) {
      throw new IllegalStateException("Could not update earliest visible message id for '" + shardPartition + "'.");
    }
  }

  @Override
  public boolean insertEarliestMessageId(TkmsShardPartition shardPartition) {
    var earliestVisibleMessages = properties.getEarliestVisibleMessages(shardPartition.getShard());
    try {
      jdbcTemplate.update("insert into " + earliestVisibleMessages.getTableName() + " (shard, part, message_id) values "
          + "(?,?,?)", shardPartition.getShard(), shardPartition.getPartition(), -1L);
    } catch (DataIntegrityViolationException e) {
      return false;
    }
    return true;
  }

  @Override
  @MonitoringQuery
  public boolean hasMessagesBeforeId(TkmsShardPartition sp, Long messageId) {
    List<Long> exists = jdbcTemplate.queryForList("select 1 from " + getTableName(sp) + " where id < ? limit 1", Long.class, messageId);
    return !exists.isEmpty();
  }

  protected abstract boolean doesEarliestVisibleMessagesTableExist();

  protected void deleteMessages0(TkmsShardPartition shardPartition, List<Long> ids) {
    int processedCount = 0;

    for (int batchSize : batchSizes) {
      while (ids.size() - processedCount >= batchSize) {
        Pair<TkmsShardPartition, Integer> p = ImmutablePair.of(shardPartition, batchSize);
        String sql = deleteSqlsMap.get(p);

        int finalProcessedCount = processedCount;
        jdbcTemplate.update(sql, ps -> {
          for (int i = 0; i < batchSize; i++) {
            Long id = ids.get(finalProcessedCount + i);
            ps.setLong(i + 1, id);
          }
        });

        processedCount += batchSize;

        metricsTemplate.recordDaoMessagesDeletion(shardPartition, batchSize);
      }
    }
  }

  protected abstract String getInsertSql(TkmsShardPartition shardPartition);

  protected abstract String getSelectSql(TkmsShardPartition shardPartition);

  /**
   * String manipulation is one of the most expensive operations, but we don't do caching here.
   *
   * <p>A Method calling this method should cache the result itself.
   */
  protected String getTableName(TkmsShardPartition shardPartition) {
    return properties.getTableBaseName() + "_" + shardPartition.getShard() + "_" + shardPartition.getPartition();
  }

  protected String getTableNameWithoutSchema(TkmsShardPartition shardPartition) {
    String tableName = getTableName(shardPartition);

    if (StringUtils.contains(tableName, ".")) {
      return StringUtils.substringAfter(tableName, ".");
    }
    return tableName;
  }

  protected String getSchemaName(TkmsShardPartition shardPartition) {
    String tableName = getTableName(shardPartition);

    if (StringUtils.contains(tableName, ".")) {
      return StringUtils.substringBefore(tableName, ".");
    }
    return currentSchema;
  }

  protected abstract String getCurrentSchema();
}
