package com.transferwise.kafka.tkms.dao;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.TkmsMessageWithSequence;
import com.transferwise.kafka.tkms.api.ShardPartition;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.config.TkmsDataSourceProvider;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.metrics.IMetricsTemplate;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Headers;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Headers.Builder;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Message;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Transactional(rollbackFor = Exception.class)
public class TkmsDao implements ITkmsDao {

  protected static final int FLAG_COMPRESS = 1;

  private static final int[] batchSizes = {256, 64, 16, 4, 1};

  protected Map<ShardPartition, String> insertMessageSqls;

  protected Map<ShardPartition, String> getMessagesSqls;

  private Map<Pair<ShardPartition, Integer>, String> deleteSqlsMap;

  @Autowired
  protected TkmsDataSourceProvider dataSourceProvider;

  @Autowired
  protected TkmsProperties properties;

  @Autowired
  protected IMetricsTemplate metricsTemplate;

  protected JdbcTemplate jdbcTemplate;

  @PostConstruct
  public void init() {
    jdbcTemplate = new JdbcTemplate(dataSourceProvider.getDataSource());

    Map<ShardPartition, String> map = new HashMap<>();
    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(s); p++) {
        ShardPartition sp = ShardPartition.of(s, p);
        map.put(sp, getInsertSql(sp));
      }
    }
    insertMessageSqls = ImmutableMap.copyOf(map);

    map.clear();
    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(s); p++) {
        ShardPartition sp = ShardPartition.of(s, p);
        map.put(sp, getSelectSql(sp));
      }
    }
    getMessagesSqls = ImmutableMap.copyOf(map);

    deleteSqlsMap = new HashMap<>();

    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(s); p++) {
        ShardPartition sp = ShardPartition.of(s, p);
        for (int batchSize : batchSizes) {
          Pair<ShardPartition, Integer> key = ImmutablePair.of(sp, batchSize);
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

  @Override
  public List<InsertMessageResult> insertMessages(ShardPartition shardPartition, List<TkmsMessageWithSequence> tkmsMessages) {
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
              ps.setBytes(1, messageToContent(shardPartition, tkmsMessageWithSequence.getTkmsMessage()));
              ps.addBatch();

              results.add(new InsertMessageResult().setSequence(tkmsMessageWithSequence.getSequence()));
            }

            ps.executeBatch();

            ResultSet rs = ps.getGeneratedKeys();
            try {
              int i = 0;
              while (rs.next()) {
                Long id = rs.getLong(1);
                results.get(idx.intValue() + i++).setStorageId(id);
                metricsTemplate.recordDaoMessageInsert(shardPartition);
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

  @Override
  public InsertMessageResult insertMessage(ShardPartition shardPartition, TkmsMessage message) {
    final InsertMessageResult result = new InsertMessageResult().setShardPartition(shardPartition);

    final KeyHolder keyHolder = new GeneratedKeyHolder();
    jdbcTemplate.update(con -> {
      PreparedStatement ps = con.prepareStatement(insertMessageSqls.get(shardPartition), Statement.RETURN_GENERATED_KEYS);
      try {
        ps.setBytes(1, messageToContent(shardPartition, message));
        return ps;
      } catch (Exception e) {
        ps.close();
        throw e;
      }
    }, keyHolder);

    metricsTemplate.recordDaoMessageInsert(shardPartition);

    result.setStorageId(keyToLong(keyHolder));
    return result;
  }

  protected byte[] messageToContent(ShardPartition shardPartition, TkmsMessage message) {
    return ExceptionUtils.doUnchecked(() -> {
      byte[] messageBytes = convert(message).toByteArray();

      boolean useCompression = properties.useCompression(shardPartition.getShard());

      ByteArrayOutputStream os = new ByteArrayOutputStream(messageBytes.length / (useCompression ? 4 : 1) + 3);

      // 3 byte header for future use
      os.write(0);
      os.write(0);

      if (properties.useCompression(shardPartition.getShard())) {
        os.write(FLAG_COMPRESS);
        compress(messageBytes, os);
      } else {
        os.write(0);
        os.write(messageBytes);
      }
      os.close();

      return os.toByteArray();
    });
  }

  protected StoredMessage.Message convert(TkmsMessage message) {
    StoredMessage.Headers headers = null;
    if (message.getHeaders() != null && !message.getHeaders().isEmpty()) {
      Builder builder = Headers.newBuilder();
      for (TkmsMessage.Header header : message.getHeaders()) {
        builder.addHeaders(StoredMessage.Header.newBuilder().setKey(header.getKey()).setValue(ByteString.copyFrom(header.getValue())).build());
      }
      headers = builder.build();
    }

    StoredMessage.Message.Builder storedMessageBuilder = StoredMessage.Message.newBuilder();
    storedMessageBuilder.setValue(ByteString.copyFrom(message.getValue()));
    if (headers != null) {
      storedMessageBuilder.setHeaders(headers);
    }
    if (message.getPartition() != null) {
      storedMessageBuilder.setPartition(UInt32Value.of(message.getPartition()));
    }
    if (message.getTimestamp() != null) {
      storedMessageBuilder.setTimestamp(UInt64Value.of(message.getTimestamp().toEpochMilli()));
    }
    if (message.getKey() != null) {
      storedMessageBuilder.setKey(message.getKey());
    }

    storedMessageBuilder.setInsertTimestamp(UInt64Value.of(System.currentTimeMillis()));

    return storedMessageBuilder.setTopic(message.getTopic()).build();
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
  protected long keyToLong(KeyHolder keyHolder) {
    return (long) keyHolder.getKey();
  }

  @Override
  public List<MessageRecord> getMessages(ShardPartition shardPartition, int maxCount) {
    return ExceptionUtils.doUnchecked(() -> {
      long startTimeMs = System.currentTimeMillis();

      Connection con = DataSourceUtils.getConnection(dataSourceProvider.getDataSource());
      try {
        metricsTemplate.recordDaoPollGetConnection(shardPartition, startTimeMs);
        startTimeMs = System.currentTimeMillis();
        int i = 0;
        try (PreparedStatement ps = con.prepareStatement(getMessagesSqls.get(shardPartition))) {
          ps.setLong(1, maxCount);

          List<MessageRecord> records = new ArrayList<>();
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              if (i++ == 0) {
                metricsTemplate.recordDaoPollFirstResult(shardPartition, startTimeMs);
              }

              MessageRecord messageRecord = new MessageRecord();
              messageRecord.setId(rs.getLong(1));

              byte[] data = rs.getBytes(2);
              byte[] messageBytes = data;
              // Temporary check for old version not having 3 byte header.
              // Can be removed as soon as there are no earlier version of lib present.
              if (data.length >= 3 && data[0] == 0 && data[1] == 0) {
                boolean compressed = (data[2] & FLAG_COMPRESS) == FLAG_COMPRESS;
                if (compressed) {
                  messageBytes = decompress(data, 3, data.length - 3);
                } else {
                  messageBytes = Arrays.copyOfRange(messageBytes, 3, messageBytes.length);
                }
              }

              long messageParsingStartTimeMs = System.currentTimeMillis();
              Message message = Message.parseFrom(messageBytes);
              metricsTemplate.recordStoredMessageParsing(shardPartition, messageParsingStartTimeMs);
              messageRecord.setMessage(message);

              records.add(messageRecord);
            }
          }

          return records;
        } finally {
          metricsTemplate.recordDaoPollAllResults(shardPartition, i, startTimeMs);
        }
      } finally {
        DataSourceUtils.releaseConnection(con, dataSourceProvider.getDataSource());
      }
    });
  }

  @Override
  public void deleteMessage(ShardPartition shardPartition, List<Long> ids) {
    MutableInt idIdx = new MutableInt();
    while (idIdx.getValue() < ids.size()) {
      for (int batchSize : batchSizes) {
        if (ids.size() - idIdx.getValue() < batchSize) {
          continue;
        }

        Pair<ShardPartition, Integer> p = ImmutablePair.of(shardPartition, batchSize);
        String sql = deleteSqlsMap.get(p);

        jdbcTemplate.update(sql, ps -> {
          for (int i = 0; i < batchSize; i++) {
            Long id = ids.get(idIdx.getAndIncrement());
            ps.setLong(i + 1, id);
          }
        });

        metricsTemplate.recordDaoMessagesDeletion(shardPartition, batchSize);
      }
    }
  }

  protected String getInsertSql(ShardPartition shardPartition) {
    return "insert into " + getTableName(shardPartition) + " (message) values (?)";
  }

  protected String getSelectSql(ShardPartition shardPartition) {
    return "select id, message from " + getTableName(shardPartition) + " order by id limit ?";
  }

  /**
   * String manipulation is one of the most expensive operations, but we don't do caching here.
   *
   * <p>A Method calling this method should cache the result itself.
   */
  protected String getTableName(ShardPartition shardPartition) {
    return properties.getTableBaseName() + "_" + shardPartition.getShard() + "_" + shardPartition.getPartition();
  }

  protected void compress(byte[] data, ByteArrayOutputStream out) {
    Deflater deflater = new Deflater();
    deflater.setInput(data);
    deflater.finish();
    byte[] buffer = new byte[1024];
    while (!deflater.finished()) {
      int count = deflater.deflate(buffer);
      out.write(buffer, 0, count);
    }
  }

  protected byte[] decompress(byte[] data, int off, int len) {
    return ExceptionUtils.doUnchecked(() -> {
      Inflater inflater = new Inflater();
      inflater.setInput(data, off, len);
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
      byte[] buffer = new byte[1024];
      while (!inflater.finished()) {
        int count = inflater.inflate(buffer);
        if (count == 0) {
          break;
        }
        outputStream.write(buffer, 0, count);
      }
      outputStream.close();
      byte[] output = outputStream.toByteArray();
      return output;
    });
  }
}
