package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.config.TkmsProperties.EarliestVisibleMessages;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import com.transferwise.kafka.tkms.metrics.ITkmsMetricsTemplate;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class EarliestMessageTracker {

  private ITkmsDao dao;
  private TkmsShardPartition shardPartition;
  private TkmsProperties properties;
  private ITkmsMetricsTemplate metricsTemplate;

  private boolean enabled;
  private Object earliestMessageIdGauge;

  private long earliestMessageId = -1;

  private long lastCommitMs = -1;

  private EarliestMessageSlidingWindow earliestMessageSlidingWindow;

  public EarliestMessageTracker(TkmsShardPartition shardPartition, ITkmsDao dao, TkmsProperties properties, ITkmsMetricsTemplate metricsTemplate) {
    this.dao = dao;
    this.shardPartition = shardPartition;
    this.properties = properties;
    this.metricsTemplate = metricsTemplate;
  }

  public void init() {
    earliestMessageIdGauge = metricsTemplate.registerEarliestMessageId(shardPartition, () -> earliestMessageId);

    EarliestVisibleMessages earliestVisibleMessages = properties.getEarliestVisibleMessages(shardPartition.getShard());
    enabled = earliestVisibleMessages.isEnabled();

    if (!enabled) {
      return;
    }

    earliestMessageSlidingWindow = new EarliestMessageSlidingWindow(earliestVisibleMessages.getLookBackPeriod());
    Long earliestMessageIdFromDb = dao.getEarliestMessageId(shardPartition);
    earliestMessageId = earliestMessageIdFromDb == null ? -1 : earliestMessageIdFromDb;
  }

  public void shutdown() {
    metricsTemplate.unregisterMetric(earliestMessageIdGauge);
    if (!enabled) {
      return;
    }

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
      this.earliestMessageId = earliestMessageIdInWindow;
    }

    commitIfFeasible();
  }

  private void commitIfFeasible() {
    if (lastCommitMs == -1 || TkmsClockHolder.getClock().millis() - lastCommitMs > 5000) {
      commit();
    }
  }

  private void commit() {
    metricsTemplate.registerEarliestMessageIdCommit(shardPartition);
    lastCommitMs = TkmsClockHolder.getClock().millis();
    dao.saveEarliestMessageId(shardPartition, earliestMessageId);
  }
}
