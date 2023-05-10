package com.transferwise.kafka.tkms;

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
  }
}
