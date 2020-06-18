package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.ShardPartition;

public interface ITkmsZookeeperOperations {

  String getLockNodePath(ShardPartition shardPartition);
}
