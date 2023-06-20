package com.transferwise.kafka.tkms;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.UtilityClass;

/*
   Mainly used to add verbose log to investigate specfic flaky tests.
 */
@UtilityClass
public class Debug {

  /*
    To allow to understand a flaky test failure in `EarliestMessageTrackingIntTest.testIfEarliestMessageTrackerBehavesAsExpected`.
   */
  @Getter
  @Setter
  private static boolean earliestMessagesTrackerDebugEnabled = false;
}
