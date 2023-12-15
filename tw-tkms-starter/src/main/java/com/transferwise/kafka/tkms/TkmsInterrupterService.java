package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor.TaskHandle;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TkmsInterrupterService implements ITkmsInterrupterService, InitializingBean {

  @Autowired
  private IExecutorServicesProvider executorServicesProvider;
  private ScheduledTaskExecutor scheduledTaskExecutor;

  public void afterPropertiesSet() {
    this.scheduledTaskExecutor = executorServicesProvider.getGlobalScheduledTaskExecutor();
  }

  @Override
  public TaskHandle interruptAfter(Thread t, Duration duration) {
    return scheduledTaskExecutor.scheduleOnce(() -> {
      var threadName = Thread.currentThread().getName();
      try {
        Thread.currentThread().setName("tkms-interrupt");
        log.warn("Had to interrupt thread '{}'.", t.getName());
        t.interrupt();
      } finally {
        Thread.currentThread().setName(threadName);
      }
    }, duration);
  }

  @Override
  public void cancelInterruption(TaskHandle handler) {
    handler.stop();
  }
}
