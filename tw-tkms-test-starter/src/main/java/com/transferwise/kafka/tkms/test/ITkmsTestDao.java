package com.transferwise.kafka.tkms.test;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;

public interface ITkmsTestDao {
  int getMessagesCount(TkmsShardPartition shardPartition);
}
