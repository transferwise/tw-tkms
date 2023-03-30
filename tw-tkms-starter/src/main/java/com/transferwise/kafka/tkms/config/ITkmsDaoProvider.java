package com.transferwise.kafka.tkms.config;

import com.transferwise.kafka.tkms.dao.ITkmsDao;

public interface ITkmsDaoProvider {

  ITkmsDao getTkmsDao(int shard);
}
