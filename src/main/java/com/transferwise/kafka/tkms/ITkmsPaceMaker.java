package com.transferwise.kafka.tkms;

import java.time.Duration;

public interface ITkmsPaceMaker {
  void doSmallPause();

  void pauseOnError();

  Duration getLongWaitTime();
}
