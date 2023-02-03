package com.transferwise.kafka.tkms;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import org.junit.jupiter.api.Test;

class TkmsPaceMakerTest {

  @Test
  void correctPauseTimeIsCalculated() {
    var paceMaker = new TkmsPaceMaker();
    var properties = new TkmsProperties();
    paceMaker.properties = properties;
    TkmsShardPartition.init(properties);

    var pause = paceMaker.getPollingPause(TkmsShardPartition.of(0, 0), 100, 0);
    assertEquals(25, pause.toMillis());

    pause = paceMaker.getPollingPause(TkmsShardPartition.of(0, 0), 100, 100);
    assertEquals(0, pause.toMillis());

    pause = paceMaker.getPollingPause(TkmsShardPartition.of(0, 0), 100, 50);
    assertEquals(12, pause.toMillis());
  }
}
