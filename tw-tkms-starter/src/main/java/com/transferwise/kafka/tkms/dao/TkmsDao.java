package com.transferwise.kafka.tkms.dao;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.kafka.tkms.Assertions;
import com.transferwise.kafka.tkms.IProblemNotifier;
import com.transferwise.kafka.tkms.TkmsMessageWithSequence;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.MDC;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Transactional;

/**
 * READ_UNCOMMITTED isolation level is to speed up queries against information schema in MariaDb databases with high number of tenants. E.g. Custom
 * environments.
 */
@Slf4j
@RequiredArgsConstructor
public abstract class TkmsDao implements ITkmsDao, InitializingBean {

  private Map<TkmsShardPartition, String> insertMessageSqls = new ConcurrentHashMap<>();
  private Map<TkmsShardPartition, String> getMessagesSqls = new ConcurrentHashMap<>();
  private Map<Pair<TkmsShardPartition, Integer>, String> deleteSqls = new ConcurrentHashMap<>();

  private Map<TkmsShardPartition, Set<Integer>> deleteBatchSizes = new ConcurrentHashMap<>();

  protected Map<Pair<TkmsShardPartition, String>, String> sqlCache = new ConcurrentHashMap<>();

  private final DataSource dataSource;
  protected final TkmsProperties properties;
  protected final ITkmsMetricsTemplate metricsTemplate;
  private final ITkmsMessageSerializer messageSerializer;
  protected final ITransactionsHelper transactionsHelper;
  protected final IProblemNotifier problemNotifier;

  protected JdbcTemplate jdbcTemplate;
  protected String currentSchema;

  @Override
  public void afterPropertiesSet() {
    jdbcTemplate = new JdbcTemplate(dataSource);
    currentSchema = getCurrentSchema();
  }

  @Override
  public void validateDatabase(int shard) {
    var earliestVisibleMessages = properties.getEarliestVisibleMessages(shard);

    if (earliestVisibleMessages.isEnabled()) {
      if (!doesEarliestVisibleMessagesTableExist()) {
        var tableName = properties.getEarliestVisibleMessages().getTableName();
        throw new IllegalStateException(
            "Earliest visible message system is enabled for shard " + shard + ", but the table '" + tableName + "' does not exist.");
      }

      int partitions = properties.getPartitionsCount(shard);
      for (int partition = 0; partition < partitions; partition++) {
        TkmsShardPartition shardPartition = TkmsShardPartition.of(shard, partition);
        if (getEarliestMessageId(shardPartition) == null) {
          insertEarliestMessageId(shardPartition);
        }
      }
    }
  }

  @Override
  public void validateDatabase() {

  }

  @Transactional(rollbackFor = Exception.class)
  @Override
  public List<InsertMessageResult> insertMessages(TkmsShardPartition shardPartition, List<TkmsMessageWithSequence> tkmsMessages) {
    return ExceptionUtils.doUnchecked(() -> {

      List<InsertMessageResult> results = new ArrayList<>();
      var idx = new MutableInt();
      while (idx.getValue() < tkmsMessages.size()) {
        var con = DataSourceUtils.getConnection(dataSource);
        try {
          var sql = insertMessageSqls.computeIfAbsent(shardPartition, this::getInsertSql);
          var ps = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
          var closeableStreams = new ArrayList<InputStream>();
          try {
            var batchSize = Math.min(properties.getInsertBatchSize(shardPartition.getShard()), tkmsMessages.size() - idx.intValue());

            for (int i = 0; i < batchSize; i++) {
              TkmsMessageWithSequence tkmsMessageWithSequence = tkmsMessages.get(idx.intValue() + i);
              var serializedMessageStream = serializeMessage(shardPartition, tkmsMessageWithSequence.getTkmsMessage());

              closeableStreams.add(serializedMessageStream);
              ps.setBinaryStream(1, serializedMessageStream);

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
            for (var is : closeableStreams) {
              try {
                is.close();
              } catch (Throwable t) {
                log.error("Failed to close input stream.", t);
              }
            }
          }
        } finally {
          DataSourceUtils.releaseConnection(con, dataSource);
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
    var sql = insertMessageSqls.computeIfAbsent(shardPartition, k -> getInsertSql(shardPartition));

    ExceptionUtils.doUnchecked(() -> {
      try (var is = serializeMessage(shardPartition, message)) {
        jdbcTemplate.update(con -> ExceptionUtils.doUnchecked(() -> {
          PreparedStatement ps = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
          try {
            ps.setBinaryStream(1, is);
            return ps;
          } catch (Exception e) {
            ps.close();
            throw e;
          }
        }), keyHolder);
      }
    });

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
    var sql = getMessagesSqls.computeIfAbsent(shardPartition, k -> getSelectSql(shardPartition));
    var result = ExceptionUtils.doUnchecked(() -> {
      long startNanoTime = System.nanoTime();

      Connection con = DataSourceUtils.getConnection(dataSource);
      try {
        metricsTemplate.recordDaoPollGetConnection(shardPartition, startNanoTime);
        startNanoTime = System.nanoTime();
        int i = 0;

        try (PreparedStatement ps = con.prepareStatement(sql)) {
          ps.setLong(1, earliestMessageId);
          ps.setLong(2, maxCount);

          List<MessageRecord> records = new ArrayList<>();
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              if (i++ == 0) {
                metricsTemplate.recordDaoPollFirstResult(shardPartition, startNanoTime);
              }

              var messageId = rs.getLong(1);
              MDC.put(properties.getMdc().getMessageIdKey(), String.valueOf(messageId));
              try {
                MessageRecord messageRecord = new MessageRecord();
                messageRecord.setId(messageId);
                messageRecord.setMessage(messageSerializer.deserialize(shardPartition, rs.getBinaryStream(2)));

                records.add(messageRecord);
              } catch (Throwable t) {
                throw new RuntimeException(
                    "Failed to deserialize message " + messageId + ", retrieved from table '" + getTableName(shardPartition) + "'.", t);
              } finally {
                MDC.remove(properties.getMdc().getMessageIdKey());
              }
            }
          }

          return records;
        } finally {
          metricsTemplate.recordDaoPollAllResults(shardPartition, i, startNanoTime);
        }
      } finally {
        DataSourceUtils.releaseConnection(con, dataSource);
      }
    });

    if (Assertions.isLevel1()) {
      var explainPlanRows = jdbcTemplate.query(getExplainClause() + " " + sql, ps -> {
        ps.setLong(1, earliestMessageId);
        ps.setLong(2, maxCount);
      }, (rs, rowNum) -> rs.getString(1));
      var explainPlan = concatStringRows(explainPlanRows);
      Assertions.assertAlgorithm(isUsingIndexScan(explainPlan), "inefficient query plan is used: " + explainPlan);
    }

    return result;
  }

  @Override
  public List<MessageRecord> getMessages(TkmsShardPartition shardPartition, long earliestMessageId, int limit, int offset) {
    var sql = sqlCache.computeIfAbsent(Pair.of(shardPartition, "getMessagesWithOffset"), k -> getSelectWithOffsetSql(shardPartition));
    var result = ExceptionUtils.doUnchecked(() -> {
      long startNanoTime = System.nanoTime();

      Connection con = DataSourceUtils.getConnection(dataSource);
      try {
        metricsTemplate.recordDaoPollGetConnection(shardPartition, startNanoTime);
        startNanoTime = System.nanoTime();
        int i = 0;

        try (PreparedStatement ps = con.prepareStatement(sql)) {
          ps.setLong(1, earliestMessageId);
          ps.setLong(2, limit);
          ps.setLong(3, offset);

          List<MessageRecord> records = new ArrayList<>();
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              if (i++ == 0) {
                metricsTemplate.recordDaoPollFirstResult(shardPartition, startNanoTime);
              }

              var messageId = rs.getLong(1);
              MDC.put(properties.getMdc().getMessageIdKey(), String.valueOf(messageId));
              try {
                MessageRecord messageRecord = new MessageRecord();
                messageRecord.setId(messageId);
                messageRecord.setMessage(messageSerializer.deserialize(shardPartition, rs.getBinaryStream(2)));

                records.add(messageRecord);
              } catch (Throwable t) {
                throw new RuntimeException(
                    "Failed to deserialize message " + messageId + ", retrieved from table '" + getTableName(shardPartition) + "'.", t);
              } finally {
                MDC.remove(properties.getMdc().getMessageIdKey());
              }
            }
          }

          return records;
        } finally {
          metricsTemplate.recordDaoPollAllResults(shardPartition, i, startNanoTime);
        }
      } finally {
        DataSourceUtils.releaseConnection(con, dataSource);
      }
    });

    return result;
  }

  @Override
  public void deleteMessages(TkmsShardPartition shardPartition, List<Long> ids) {
    var batchSizeExists =
        deleteBatchSizes.computeIfAbsent(shardPartition, k -> new HashSet<>(properties.getDeleteBatchSizes(k.getShard()))).contains(ids.size());

    if (batchSizeExists) {
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
    var sql = sqlCache.computeIfAbsent(Pair.of(shardPartition, "getEarliestMessageId"), k -> getEarliestMessageIdSql(shardPartition));
    var ids = jdbcTemplate.queryForList(sql, Long.class, shardPartition.getShard(), shardPartition.getPartition());
    return ids.isEmpty() ? null : ids.get(0);
  }

  protected abstract String getEarliestMessageIdSql(TkmsShardPartition shardPartition);

  @Override
  public void saveEarliestMessageId(TkmsShardPartition shardPartition, long messageId) {
    var sql = sqlCache.computeIfAbsent(Pair.of(shardPartition, "saveEarliestMessageId"), k -> {
      var earliestVisibleMessages = properties.getEarliestVisibleMessages(shardPartition.getShard());
      return "update " + earliestVisibleMessages.getTableName() + " set message_id=? where shard=? and part=?";
    });

    if (jdbcTemplate.update(sql, messageId, shardPartition.getShard(), shardPartition.getPartition()) != 1) {
      throw new IllegalStateException("Could not update earliest visible message id for '" + shardPartition + "'.");
    }
  }

  @Override
  public boolean insertEarliestMessageId(TkmsShardPartition shardPartition) {
    var sql = sqlCache.computeIfAbsent(Pair.of(shardPartition, "insertEarliestMessageId"), k -> {
      var earliestVisibleMessages = properties.getEarliestVisibleMessages(shardPartition.getShard());
      return "insert into " + earliestVisibleMessages.getTableName() + " (shard, part, message_id) values (?,?,?)";
    });

    try {
      jdbcTemplate.update(sql, shardPartition.getShard(), shardPartition.getPartition(), -1L);
    } catch (DataIntegrityViolationException e) {
      return false;
    }
    return true;
  }

  @Override
  public boolean hasMessagesBeforeId(TkmsShardPartition shardPartition, Long messageId) {
    var sql = sqlCache.computeIfAbsent(Pair.of(shardPartition, "hasMessagesBeforeId"), k -> getHasMessagesBeforeIdSql(shardPartition));
    List<Long> exists = jdbcTemplate.queryForList(sql, Long.class, messageId);
    var result = !exists.isEmpty();

    if (Assertions.isLevel1()) {
      var rows = jdbcTemplate.queryForList(getExplainClause() + " " + sql, String.class, messageId);
      var explainPlan = concatStringRows(rows);
      Assertions.assertAlgorithm(isUsingIndexScan(explainPlan), "inefficient query plan is used: " + explainPlan);
    }

    return result;
  }

  protected abstract String getHasMessagesBeforeIdSql(TkmsShardPartition shardPartition);

  protected abstract boolean doesEarliestVisibleMessagesTableExist();

  protected void deleteMessages0(TkmsShardPartition shardPartition, List<Long> ids) {
    int processedCount = 0;

    for (int batchSize : properties.getDeleteBatchSizes(shardPartition.getShard())) {
      while (ids.size() - processedCount >= batchSize) {
        Pair<TkmsShardPartition, Integer> p = ImmutablePair.of(shardPartition, batchSize);
        String sql = deleteSqls.computeIfAbsent(p, k -> getDeleteSql(shardPartition, batchSize));

        int finalProcessedCount = processedCount;
        jdbcTemplate.update(sql, ps -> {
          for (int i = 0; i < batchSize; i++) {
            Long id = ids.get(finalProcessedCount + i);
            ps.setLong(i + 1, id);
          }
        });

        if (Assertions.isLevel1()) {
          var explainPlanRows = jdbcTemplate.query(getExplainClause() + " " + sql, ps -> {
            for (int i = 0; i < batchSize; i++) {
              Long id = ids.get(finalProcessedCount + i);
              ps.setLong(i + 1, id);
            }
          }, (rs, rowNum) -> rs.getString(1));
          var explainPlan = concatStringRows(explainPlanRows);
          Assertions.assertAlgorithm(isUsingIndexScan(explainPlan), "inefficient query plan is used: " + explainPlan);
        }

        processedCount += batchSize;

        metricsTemplate.recordDaoMessagesDeletion(shardPartition, batchSize);
      }
    }
  }

  protected abstract String getExplainClause();

  protected abstract boolean isUsingIndexScan(String explainPlan);

  protected abstract String getInsertSql(TkmsShardPartition shardPartition);

  protected abstract String getSelectSql(TkmsShardPartition shardPartition);

  protected abstract String getSelectWithOffsetSql(TkmsShardPartition shardPartition);

  protected abstract String getDeleteSql(TkmsShardPartition shardPartition, int batchSize);

  /**
   * String manipulation is one of the most expensive operations, but we don't do caching here.
   *
   * <p>A Method calling this method should cache the result itself.
   */
  protected String getTableName(TkmsShardPartition shardPartition) {
    return properties.getTableBaseName(shardPartition.getShard()) + "_" + shardPartition.getShard() + "_" + shardPartition.getPartition();
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

  protected String concatStringRows(List<String> rows) {
    var sb = new StringBuilder();
    for (int i = 0; i < rows.size(); i++) {
      if (i > 0) {
        sb.append("\n");
      }
      sb.append(rows.get(i));
    }
    return sb.toString();
  }
}
