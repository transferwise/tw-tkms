package com.transferwise.kafka.tkms.demoapp;

import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PaceTracker implements GracefulShutdownStrategy, InitializingBean {

  private final AtomicLong messagesInserted = new AtomicLong();
  private final AtomicLong messagesDelivered = new AtomicLong();
  private long startTimeMs = -1;
  private int timeSlice = 0;
  private volatile boolean shuttingDown;
  private long lastSum = 0;

  @Override
  public void afterPropertiesSet() {
    new Thread(() -> {
      while (!shuttingDown) {
        try {
          stats();
          Thread.sleep(100);
        } catch (InterruptedException e) {
          log.error(e.getMessage(), e);
        }
      }
    }).start();
  }

  public synchronized void startTracking() {
    this.startTimeMs = System.currentTimeMillis();
    this.timeSlice = 0;
  }

  public void messagesInserted(int cnt) {
    messagesInserted.addAndGet(cnt);
    stats();
  }

  public void messagesDelivered(int cnt) {
    messagesDelivered.addAndGet(cnt);
    stats();
  }

  public synchronized void stats() {
    if (startTimeMs == -1) {
      return;
    }
    if (lastSum == messagesDelivered.get() + messagesInserted.get()) {
      return;
    }
    long durationMs = System.currentTimeMillis() - startTimeMs;
    if (durationMs >= (timeSlice + 1) * 10000) {
      timeSlice++;
      log.info("" + durationMs + " ms: inserted: " + messagesInserted + ", delivered: " + messagesDelivered);
      log.info("  Pace: inserted: " + (messagesInserted.get() * 1000 / durationMs) + " m/s"
          + ", delivered: " + (messagesDelivered.get() * 1000 / durationMs) + " m/s.");

      lastSum = messagesDelivered.get() + messagesInserted.get();
    }
  }

  @Override
  public boolean canShutdown() {
    shuttingDown = true;
    return true;
  }
}
