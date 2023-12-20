package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;

public interface ITkmsTopicValidator {

  void preValidateAll();

  void validate(TkmsShardPartition tkmsShardPartition, String topic, Integer partition);
}
