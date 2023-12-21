package com.transferwise.kafka.tkms.config;

import org.apache.kafka.clients.admin.Admin;

public interface ITkmsKafkaAdminProvider {

  Admin getKafkaAdmin();

  void closeKafkaAdmin();

}
