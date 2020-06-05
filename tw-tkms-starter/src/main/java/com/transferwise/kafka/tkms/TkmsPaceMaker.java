package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;

public class TkmsPaceMaker implements ITkmsPaceMaker {

  @Autowired
  private TkmsProperties properties;

  @Override
  public void doSmallPause() {
    ExceptionUtils.doUnchecked(() -> Thread.sleep(properties.getDesiredLatency().toMillis()));
  }

  @Override
  public void pauseOnError() {
    ExceptionUtils.doUnchecked(() -> Thread.sleep(properties.getPauseTimeOnErrors().toMillis()));
  }

  @Override
  public Duration getLongWaitTime() {
    return Duration.ofSeconds(15);
  }
}
