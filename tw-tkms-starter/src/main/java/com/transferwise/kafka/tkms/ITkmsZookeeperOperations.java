package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;

public interface ITkmsZookeeperOperations {

  String getLockNodePath(TkmsShardPartition shardPartition);
}
