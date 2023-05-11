package com.transferwise.kafka.tkms;

import static com.transferwise.kafka.tkms.EarliestMessageSlidingWindow.BUCKETS_COUNT;

import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.vdurmont.semver4j.Semver;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

public class BaseEnvironmentValidator implements IEnvironmentValidator {

  @Autowired
  protected TkmsProperties properties;

  @Override
  public void validate() {
    String previousVersion = properties.getEnvironment().getPreviousVersion();
    if (StringUtils.trimToNull(previousVersion) == null) {
      throw new IllegalStateException("Previous version has not been specified.");
    }

    Semver previousSemver = new Semver(previousVersion);
    Semver minimumRequiredPreviousSemver = new Semver("0.7.3");

    if (previousSemver.compareTo(minimumRequiredPreviousSemver) < 0) {
      throw new IllegalStateException("This version requires at least '" + minimumRequiredPreviousSemver + "' to be deployed first.");
    }

    for (int s = 0; s < properties.getShardsCount(); s++) {
      var earliestVisibleMessages = properties.getEarliestVisibleMessages(s);

      var loopBackPeriodMs = earliestVisibleMessages.getLookBackPeriod().toMillis();
      long stepMs = loopBackPeriodMs / BUCKETS_COUNT;
      if (stepMs * BUCKETS_COUNT != loopBackPeriodMs) {
        throw new IllegalStateException("LoopBackPeriod in millis for shard " + s + " is not dividable by " + BUCKETS_COUNT);
      }
    }
  }
}
