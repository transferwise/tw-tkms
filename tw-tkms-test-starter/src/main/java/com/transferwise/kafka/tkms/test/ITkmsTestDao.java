package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.ShardPartition;

public interface ITkmsTestDao {
  int getMessagesCount(ShardPartition shardPartition);
}
