package com.transferwise.kafka.tkms;

public interface ITkmsZookeeperOperations {
  String getLockNodePath(int shard);
}
