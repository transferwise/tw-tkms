package com.transferwise.kafka.tkms;


public interface ITkmsMessagePollerFactory {

  ITkmsMessagePoller createPoller(int shard);

}
