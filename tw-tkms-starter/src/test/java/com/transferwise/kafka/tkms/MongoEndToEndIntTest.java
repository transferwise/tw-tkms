package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"test", "mongo"})
@Slf4j
public class MongoEndToEndIntTest extends EndToEndIntTest {

  @Autowired
  protected MongoTemplate mongoTemplate;

  @Override
  @Ignore
  public void testMessageIsCompressed(CompressionAlgorithm algorithm, int expectedSerializedSize) throws Exception {
    super.testMessageIsCompressed(algorithm, expectedSerializedSize);
  }

  @Override
  protected int getTablesRowsCount() {
    int count = 0;
    for (int s = 0; s < tkmsProperties.getShardsCount(); s++) {
      for (int p = 0; p < tkmsProperties.getPartitionsCount(s); p++) {
        TkmsShardPartition sp = TkmsShardPartition.of(s, p);
        count += getMessagesCount(sp);
      }
    }
    return count;
  }

  public int getMessagesCount(TkmsShardPartition shardPartition) {
    Query query = new Query(Criteria.where("_id").exists(true));
    long count = mongoTemplate.count(query, getTableName(shardPartition));
    System.out.println("table : " + getTableName(shardPartition) + " Count: " + count);
    return (int) count;
  }

  protected String getTableName(TkmsShardPartition shardPartition) {
    return tkmsProperties.getTableBaseName() + "_" + shardPartition.getShard() + "_" + shardPartition.getPartition();
  }

}
