package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NotThreadSafe
@RequiredArgsConstructor
@Slf4j
public class EarliestMessageTracker {

  private final ITkmsDao tkmsDao;
  private final TkmsShardPartition shardPartition;
  private final TkmsProperties properties;
  private final ITkmsMetricsTemplate metricsTemplate;

  private boolean enabled;
  private Object earliestMessageIdGauge;

  private volatile long earliestMessageId = -1;

  private long lastCommitMs = -1;

  private EarliestMessageSlidingWindow earliestMessageSlidingWindow;

  public void init() {
    var earliestVisibleMessages = properties.getEarliestVisibleMessages(shardPartition.getShard());
    enabled = earliestVisibleMessages.isEnabled();

    if (!enabled) {
      return;
    }

    earliestMessageIdGauge = metricsTemplate.registerEarliestMessageId(shardPartition, () -> earliestMessageId);

    earliestMessageSlidingWindow = new EarliestMessageSlidingWindow(earliestVisibleMessages.getLookBackPeriod());
    var earliestMessageIdFromDb = tkmsDao.getEarliestMessageId(shardPartition);
    earliestMessageId = earliestMessageIdFromDb == null ? -1 : earliestMessageIdFromDb;
  }

  public void shutdown() {
    if (!enabled) {
      return;
    }

    metricsTemplate.unregisterMetric(earliestMessageIdGauge);

    commit();
  }

  public Long getEarliestMessageId() {
    return earliestMessageId;
  }

  public void register(long id) {
    if (!enabled) {
      return;
    }

    earliestMessageSlidingWindow.register(id);

    long earliestMessageIdInWindow = earliestMessageSlidingWindow.getEarliestMessageId();
    if (earliestMessageIdInWindow != -1) {
      if (this.earliestMessageId != earliestMessageIdInWindow) {
        if (Debug.isEarliestMessagesTrackerDebugEnabled()) {
          log.info("Setting earliestMessageId=" + earliestMessageIdInWindow);
        }
        this.earliestMessageId = earliestMessageIdInWindow;
        commitIfFeasible();
      }
    }
  }

  private void commitIfFeasible() {
    if (lastCommitMs == -1 || TkmsClockHolder.getClock().millis() - lastCommitMs > 5_000) {
      commit();
    }
  }

  private void commit() {
    metricsTemplate.registerEarliestMessageIdCommit(shardPartition);
    lastCommitMs = TkmsClockHolder.getClock().millis();
    tkmsDao.saveEarliestMessageId(shardPartition, earliestMessageId);
  }
}
