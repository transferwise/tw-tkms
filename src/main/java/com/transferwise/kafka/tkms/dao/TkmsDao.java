package com.transferwise.kafka.tkms.dao;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.Message;
import com.transferwise.kafka.tkms.TkmsProperties;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Headers;
import com.transferwise.kafka.tkms.stored_message.StoredMessage.Headers.Builder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
public class TkmsDao implements ITkmsDao {

  private static final int[] batchSizes = {256, 64, 16, 4, 1};

  protected Map<Integer, String> insertMessageSqls;

  protected Map<Integer, String> getMessagesSqls;

  private Map<Pair<Integer, Integer>, String> deleteSqlsMap;

  @Autowired
  protected DataSource dataSource;

  @Autowired
  protected TkmsProperties properties;

  @Autowired
  protected MeterRegistry meterRegistry;

  protected JdbcTemplate jdbcTemplate;

  @PostConstruct
  public void init() {
    jdbcTemplate = new JdbcTemplate(dataSource);

    Map<Integer, String> map = new HashMap<>();
    for (int i = 0; i < properties.getShardsCount(); i++) {
      map.put(i, getInsertSql(i));
    }
    insertMessageSqls = ImmutableMap.copyOf(map);

    map.clear();
    for (int i = 0; i < properties.getShardsCount(); i++) {
      map.put(i, "select id, message from " + properties.getTableBaseName() + "_" + i + " order by id limit ?");
    }
    getMessagesSqls = ImmutableMap.copyOf(map);

    deleteSqlsMap = new HashMap<>();
    for (int i = 0; i < properties.getShardsCount(); i++) {
      int shard = i;
      for (int bz = 0; bz < batchSizes.length; bz++) {
        int batchSize = batchSizes[bz];
        Pair<Integer, Integer> p = ImmutablePair.of(shard, batchSize);
        StringBuilder sb = new StringBuilder("delete from " + properties.getTableBaseName() + "_" + shard + " where id in (");
        for (int j = 0; j < batchSize; j++) {
          sb.append("?");
          if (j < batchSize - 1) {
            sb.append(",");
          }
        }
        deleteSqlsMap.put(p, sb.append(")").toString());
      }
    }

    deleteSqlsMap = ImmutableMap.copyOf(deleteSqlsMap);
  }

  @Override
  public long insertMessage(Message message) {
    KeyHolder keyHolder = new GeneratedKeyHolder();

    int shard = getShard(message);

    StoredMessage.Headers headers = null;
    if (message.getHeaders() != null && !message.getHeaders().isEmpty()) {
      Builder builder = Headers.newBuilder();
      for (Message.Header header : message.getHeaders()) {
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

    StoredMessage.Message storedMessage = storedMessageBuilder.setTopic(message.getTopic()).build();

    byte[] messageBytes = storedMessage.toByteArray();

    jdbcTemplate.update(con -> {
      PreparedStatement ps = con.prepareStatement(insertMessageSqls.get(shard), Statement.RETURN_GENERATED_KEYS);
      ps.setBytes(1, messageBytes);
      return ps;
    }, keyHolder);

    meterRegistry.counter("tw.tkms.dao.insert.message", "shard", String.valueOf(shard)).increment();

    return (long) keyHolder.getKey();
  }

  @Override
  public List<MessageRecord> getMessages(int shard, int maxCount) {
    return jdbcTemplate.query(getMessagesSqls.get(shard), ps -> {
      ps.setLong(1, maxCount);
    }, (rs, rowNum) -> ExceptionUtils.doUnchecked(() -> {
      MessageRecord messageRecord = new MessageRecord();
      messageRecord.setId(rs.getLong(1));
      messageRecord.setMessage(StoredMessage.Message.parseFrom(rs.getBytes(2)));

      return messageRecord;
    }));
  }


  @Override
  @Transactional
  public void deleteMessage(int shard, List<Long> ids) {
    MutableInt idIdx = new MutableInt();
    while (idIdx.getValue() < ids.size()) {
      for (int bz = 0; bz < batchSizes.length; bz++) {
        int batchSize = batchSizes[bz];
        if (ids.size() - idIdx.getValue() < batchSize) {
          continue;
        }

        Pair<Integer, Integer> p = ImmutablePair.of(shard, batchSize);
        String sql = deleteSqlsMap.get(p);

        jdbcTemplate.update(sql, ps -> {
          for (int i = 0; i < batchSize; i++) {
            Long id = ids.get(idIdx.getAndIncrement());
            ps.setLong(i + 1, id);
          }
        });

        Tags tags = Tags.of("shard", String.valueOf(shard));
        meterRegistry.counter("tw.tkms.dao.delete.queries", tags).increment();
        meterRegistry.counter("tw.tkms.dao.delete.records", tags).increment(batchSize);
      }
    }
  }


  private int getShard(Message message) {
    int tablesCount = properties.getShardsCount();
    if (tablesCount == 1) {
      return 0;
    }
    if (message.getPartition() != null) {
      return Math.abs(message.getPartition()) % tablesCount;
    }
    if (message.getKey() != null) {
      return Math.abs(message.getKey().hashCode()) % tablesCount;
    }
    return ThreadLocalRandom.current().nextInt(tablesCount);
  }

  protected String getInsertSql(int shard) {
    return "insert into " + properties.getTableBaseName() + "_" + shard + " (message) values (?)";
  }
}
