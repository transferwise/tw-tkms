package com.transferwise.kafka.tkms.dao;

import static com.transferwise.kafka.tkms.dao.TkmsDao.batchSizes;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;

import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.TkmsMessageWithSequence;
import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import com.transferwise.kafka.tkms.stored_message.StoredMessage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
public class TkmsMongoDao implements ITkmsDao {

  @Autowired
  protected TkmsProperties properties;
  @Autowired
  protected MongoTemplate mongoTemplate;
  @Autowired
  protected ITkmsMetricsTemplate metricsTemplate;

  @PostConstruct
  public void init() {
    checkDbConcernSettings(mongoTemplate.getDb());
    createCollections();
  }

  private void createCollections() {
    final int shardsCount = properties.getShardsCount();
    final int partitionCount = properties.getPartitionsCount();
    final String tableName = properties.getTableBaseName();

    for (int currentShard = 0; currentShard < shardsCount; currentShard++) {
      for (int currentPartition = 0; currentPartition < partitionCount; currentPartition++) {
        String collectionName = getCollectionName(tableName, currentShard, currentPartition);
        if (!mongoTemplate.collectionExists(collectionName)) {
          mongoTemplate.createCollection(collectionName);
          log.info("Created MongoDB collection [{}] to be used for tkms", collectionName);
          createIndex(collectionName);
        }
      }
    }
  }

  private String getCollectionName(final TkmsShardPartition shardPartition) {
    return getCollectionName(
        properties.getTableBaseName(), shardPartition.getShard(), shardPartition.getPartition());
  }

  private String getCollectionName(final String tableName, int shard, int partition) {
    return tableName + "_" + shard + "_" + partition;
  }

  private void createIndex(String collectionName) {
    MongoCollection<Document> mongoCollection = mongoTemplate.getCollection(collectionName);
    IndexOptions indexOptions = new IndexOptions().background(true);
    Bson index = Indexes.compoundIndex(
        Indexes.text(collectionName + ".topic"),
        Indexes.text(collectionName + ".key"));
    mongoCollection.createIndex(index, indexOptions);
    log.info("Created index '{}' on mongo DB collection '{}'", index, collectionName);
  }

  @Transactional(rollbackFor = Exception.class)
  @Override
  public InsertMessageResult insertMessage(TkmsShardPartition shardPartition, TkmsMessage message) {
    OutgoingMessage savedOutgoingMessage =
        mongoTemplate.insert(generateOutgoingMessage(message), getCollectionName(shardPartition));

    final InsertMessageResult result = new InsertMessageResult().setShardPartition(shardPartition);

    metricsTemplate.recordDaoMessageInsert(shardPartition, message.getTopic());

    result.setStorageId(savedOutgoingMessage.getId().toString());
    return result;
  }

  @Transactional(rollbackFor = Exception.class)
  @Override
  public List<InsertMessageResult> insertMessages(TkmsShardPartition shardPartition,
                                                  List<TkmsMessageWithSequence> tkmsMessages) {
    return ExceptionUtils.doUnchecked(() -> {

      List<InsertMessageResult> results = new ArrayList<>();
      MutableInt idx = new MutableInt();
      while (idx.getValue() < tkmsMessages.size()) {
        int batchSize = Math.min(
            properties.getInsertBatchSize(shardPartition.getShard()), tkmsMessages.size() - idx.intValue());

        final List<OutgoingMessage> batchOfMessages = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
          TkmsMessageWithSequence tkmsMessageWithSequence = tkmsMessages.get(idx.intValue() + i);
          batchOfMessages.add(generateOutgoingMessage(tkmsMessageWithSequence.getTkmsMessage()));
          results.add(new InsertMessageResult().setSequence(tkmsMessageWithSequence.getSequence()));
        }

        List<OutgoingMessage> savedOutgoingMessages =
            (List<OutgoingMessage>) mongoTemplate.insert(batchOfMessages, getCollectionName(shardPartition));

        int i = 0;
        for (OutgoingMessage outgoingMessage : savedOutgoingMessages) {
          String id = outgoingMessage.getId().toString();
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
        idx.add(batchSize);
      }
      return results;
    });
  }

  private OutgoingMessage generateOutgoingMessage(final TkmsMessage tkmsMessage) {
    final List<MessageHeader> messageHeaders = new ArrayList<>();
    if (!isEmpty(tkmsMessage.getHeaders())) {
      tkmsMessage.getHeaders().forEach(header ->
          messageHeaders.add(
              MessageHeader.builder()
                  .key(header.getKey())
                  .value(new Binary(header.getValue()))
                  .build()));
    }

    return OutgoingMessage.builder()
        .topic(tkmsMessage.getTopic())
        .key(tkmsMessage.getKey())
        .headers(messageHeaders)
        .partition(tkmsMessage.getPartition())
        .timestamp(tkmsMessage.getTimestamp())
        .insertTimestamp(Instant.now())
        .value(new Binary(tkmsMessage.getValue()))
        .build();
  }

  private StoredMessage.Message toStoredMessage(final OutgoingMessage outgoingMessage) {

    List<StoredMessage.Header> headerList = outgoingMessage.getHeaders().stream()
        .map(messageHeader ->
            StoredMessage.Header.newBuilder()
                .setKey(messageHeader.getKey())
                .setValue(ByteString.copyFrom(messageHeader.getValue().getData())).build())
        .collect(Collectors.toList());

    StoredMessage.Message.Builder builder = StoredMessage.Message.newBuilder();
    builder
        .setTopic(outgoingMessage.getTopic())
        .setValue(ByteString.copyFrom(outgoingMessage.getValue().getData()))
        .setHeaders(StoredMessage.Headers.newBuilder().addAllHeaders(headerList).build())
        .setInsertTimestamp(UInt64Value.newBuilder().setValue(outgoingMessage.getInsertTimestamp().toEpochMilli()).build());

    if (outgoingMessage.getPartition() != null) {
      builder.setPartition(UInt32Value.newBuilder().setValue(outgoingMessage.getPartition()).build());
    }
    if (outgoingMessage.getKey() != null) {
      builder.setKey(outgoingMessage.getKey());
    }
    if (outgoingMessage.getTimestamp() != null) {
      builder.setTimestamp(UInt64Value.newBuilder().setValue(outgoingMessage.getTimestamp().toEpochMilli()).build());
    }
    return builder.build();
  }

  @Override
  public List<MessageRecord> getMessages(TkmsShardPartition shardPartition, int maxCount) {
    return ExceptionUtils.doUnchecked(() -> {
      long startNanoTime = System.nanoTime();
      metricsTemplate.recordDaoPollGetConnection(shardPartition, startNanoTime);
      startNanoTime = System.nanoTime();
      int i = 0;
      try {
        Query query = new Query(Criteria.where("id").exists(true)).limit(maxCount);
        List<OutgoingMessage> outgoingMessages = mongoTemplate.find(query, OutgoingMessage.class, getCollectionName(shardPartition));
        List<MessageRecord> records = new ArrayList<>();

        outgoingMessages.forEach(outgoingMessage -> {
          MessageRecord messageRecord = new MessageRecord();
          messageRecord.setId(outgoingMessage.getId().toString());
          messageRecord.setMessage(toStoredMessage(outgoingMessage));
          records.add(messageRecord);
        });
        return records;
      } finally {
        metricsTemplate.recordDaoPollAllResults(shardPartition, i, startNanoTime);
      }
    });
  }

  @Transactional(rollbackFor = Exception.class)
  @Override
  public void deleteMessages(TkmsShardPartition shardPartition, List<String> records) {
    int processedCount = 0;
    for (int batchSize : batchSizes) {

      while (records.size() - processedCount >= batchSize) {
        int finalProcessedCount = processedCount;
        List<ObjectId> objectIdsToDelete = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
          String id = records.get(finalProcessedCount + i);
          objectIdsToDelete.add(new ObjectId(id));
        }
        Query query = new Query(new Criteria("id").in(objectIdsToDelete));
        mongoTemplate.remove(query, OutgoingMessage.class, getCollectionName(shardPartition));
        processedCount += batchSize;

        metricsTemplate.recordDaoMessagesDeletion(shardPartition, batchSize);
      }
    }
  }

  private void checkDbConcernSettings(MongoDatabase mongoDatabase) {
    if (!ReadConcern.MAJORITY.equals(mongoDatabase.getReadConcern())
        || !WriteConcern.MAJORITY.equals(mongoDatabase.getWriteConcern())
        || !ReadPreference.primary().equals(mongoDatabase.getReadPreference())) {

      String writeConcern = null;
      if (mongoDatabase.getWriteConcern().getWObject() != null) {
        if (mongoDatabase.getWriteConcern().getWObject() instanceof String) {
          writeConcern = mongoDatabase.getWriteConcern().getWString();
        } else {
          writeConcern = String.valueOf(mongoDatabase.getWriteConcern().getW());
        }
      }
      String readConcern = mongoDatabase.getReadConcern().getLevel() != null
          ? mongoDatabase.getReadConcern().getLevel().getValue() : "DEFAULT";

      log.warn("Using concern configuration that does not guarantee consistency. readConcern={}, "
              + "writeConcern={}, readPreference={}", readConcern, writeConcern,
          mongoDatabase.getReadPreference().getName());
    }
  }

  @Builder
  @Getter
  private static class OutgoingMessage {
    private ObjectId id;
    /* hashed shardKey composed of topic and key */
    private String topic;
    private Integer partition;
    private String key;
    private Instant timestamp;
    private Instant insertTimestamp;
    private Binary value;
    private List<MessageHeader> headers;
  }

  @Builder
  @Getter
  private static class MessageHeader {
    public String key;
    public Binary value;
  }
}
