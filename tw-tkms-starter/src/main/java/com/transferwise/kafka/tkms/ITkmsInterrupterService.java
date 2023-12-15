package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor.TaskHandle;
import java.time.Duration;

public interface ITkmsInterrupterService {

  TaskHandle interruptAfter(Thread t, Duration duration);

  /**
   * Cancels the previously set interruption task.
   *
   * <p>The handle has to be the one returned from the `interruptAfter` call.
   */
  void cancelInterruption(TaskHandle handler);

}
