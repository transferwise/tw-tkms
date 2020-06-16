package com.transferwise.kafka.tkms;

import java.time.Duration;

public interface ITkmsPaceMaker {

  void doSmallPause(int shard);

  void pauseOnError(int shard);

  Duration getLongWaitTime(int shard);
}
