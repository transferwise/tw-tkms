package com.transferwise.kafka.tkms.metrics;

import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor.TaskHandle;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.common.leaderselector.ILock;
import com.transferwise.common.leaderselector.LeaderSelectorV2;
import com.transferwise.common.leaderselector.SharedReentrantLockBuilderFactory;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.dao.ITkmsDao;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableObject;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TkmsClusterWideStateMonitor implements GracefulShutdownStrategy {

  @Autowired
  private ITkmsDao tkmsDao;
  @Autowired
  private SharedReentrantLockBuilderFactory lockBuilderFactory;
  @Autowired
  private ITkmsMetricsTemplate coreMetricsTemplate;
  @Autowired
  private TkmsProperties properties;
  @Autowired
  private IExecutorServicesProvider executorServicesProvider;
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;

  private LeaderSelectorV2 leaderSelector;
  private final Lock stateLock = new ReentrantLock();

  private Map<TkmsShardPartition, AtomicLong> approximateMessagesCount;
  private List<Object> registeredMetricHandles;
  private boolean initialized;

  @PostConstruct
  public void init() {
    String nodePath = "/tw/tw_tkms/" + properties.getGroupId() + "/tasks_state_monitor";

    ExecutorService executorService = new ThreadNamingExecutorServiceWrapper("tw-tasks-tsm", executorServicesProvider.getGlobalExecutorService());
    ILock lock = lockBuilderFactory.createBuilder(nodePath).build();
    leaderSelector = new LeaderSelectorV2.Builder().setLock(lock).setExecutorService(executorService).setLeader(control -> {
      ScheduledTaskExecutor scheduledTaskExecutor = executorServicesProvider.getGlobalScheduledTaskExecutor();
      MutableObject<TaskHandle> taskHandleHolder = new MutableObject<>();

      control.workAsyncUntilShouldStop(
          () -> {
            resetState(true);
            TkmsProperties.Monitoring monitoring = properties.getMonitoring();
            taskHandleHolder.setValue(scheduledTaskExecutor
                .scheduleAtFixedInterval(this::check, monitoring.getStartDelay(),
                    monitoring.getInterval()));
            log.info("Started to monitor tasks state for '" + properties.getGroupId() + "'.");
          },
          () -> {
            log.info("Stopping monitoring of tasks state for '" + properties.getGroupId() + "'.");
            if (taskHandleHolder.getValue() != null) {
              taskHandleHolder.getValue().stop();
              taskHandleHolder.getValue().waitUntilStopped(Duration.ofMinutes(1));
            }
            resetState(false);
            log.info("Monitoring of tasks state stopped.");
          });
    }).build();

    coreMetricsTemplate.registerLibrary();
  }

  protected void resetState(boolean forInit) {
    unitOfWorkManager.createEntryPoint("TKMS", "MonitorReset").toContext().execute(
        () -> {
          stateLock.lock();
          try {
            /*
              The main idea between unregistering the metrics, is to not left 0 or old values lying around in Grafana but make this metric disappear
              from current node.
              This will make the picture much more clear and accurate.
            */
            if (registeredMetricHandles != null) {
              for (Object metricHandle : registeredMetricHandles) {
                coreMetricsTemplate.unregisterMetric(metricHandle);
              }
            }

            registeredMetricHandles = new ArrayList<>();
            approximateMessagesCount = new HashMap<>();

            initialized = forInit;
          } finally {
            stateLock.unlock();
          }
        });
  }

  protected void check() {
    unitOfWorkManager.createEntryPoint("TKMS", "MonitorCheck").toContext().execute(
        () -> {
          stateLock.lock();
          try {
            if (!initialized) {
              return;
            }
            checkApproximateMessagesCount();
          } finally {
            stateLock.unlock();
          }
        });
  }

  protected void checkApproximateMessagesCount() {
    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(p); p++) {
        TkmsShardPartition sp = TkmsShardPartition.of(s, p);

        long count = tkmsDao.getApproximateMessagesCount(sp);

        approximateMessagesCount.computeIfAbsent(sp, k -> {
          AtomicLong counter = new AtomicLong(count);
          registeredMetricHandles.add(coreMetricsTemplate.registerApproximateMessagesCount(sp, counter::get));
          return counter;
        }).set(count);
      }
    }
  }

  @Override
  public void applicationStarted() {
    leaderSelector.start();
  }

  @Override
  public void prepareForShutdown() {
    if (leaderSelector != null) {
      leaderSelector.stop();
    }
  }

  @Override
  public boolean canShutdown() {
    return leaderSelector == null || leaderSelector.hasStopped();
  }
}
