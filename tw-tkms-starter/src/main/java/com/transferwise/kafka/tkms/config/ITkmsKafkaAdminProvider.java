package com.transferwise.kafka.tkms.config;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import org.apache.kafka.clients.admin.Admin;

public interface ITkmsKafkaAdminProvider {

  Admin getKafkaAdmin(TkmsShardPartition tkmsShardPartition);

  void closeKafkaAdmin(TkmsShardPartition tkmsShardPartition);

}
