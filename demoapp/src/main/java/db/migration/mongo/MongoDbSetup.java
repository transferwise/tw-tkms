package db.migration.mongo;

import com.github.cloudyrock.mongock.ChangeLog;
import com.github.cloudyrock.mongock.ChangeSet;
import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import io.changock.migration.api.annotations.NonLockGuarded;
import lombok.extern.slf4j.Slf4j;

@ChangeLog(order = "001")
@Slf4j
public class MongoDbSetup {

  @ChangeSet(order = "001", id = "setUpTkmsCollections", author = "mongock")
  public void setUpTkmsCollections(MongockTemplate mongockTemplate, @NonLockGuarded TkmsProperties properties) {
    final int shardsCount = properties.getShardsCount();
    final int partitionCount = properties.getPartitionsCount();
    final String tableName = properties.getTableBaseName();

    for (int currentShard = 0; currentShard < shardsCount; currentShard++) {
      for (int currentPartition = 0; currentPartition < partitionCount; currentPartition++) {
        String collectionName = getCollectionName(tableName, currentShard, currentPartition);
        if (!mongockTemplate.collectionExists(collectionName)) {
          mongockTemplate.createCollection(collectionName);
          log.info("Created MongoDB collection [{}] to be used for tkms", collectionName);
        }
      }
    }
  }

  private String getCollectionName(final String tableName, int shard, int partition) {
    return tableName + "_" + shard + "_" + partition;
  }
}

