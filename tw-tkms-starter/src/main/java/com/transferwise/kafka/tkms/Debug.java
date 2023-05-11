package com.transferwise.kafka.tkms;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Debug {
  
  /*
    To allow to understand a flaky test failure.
   */
  @Getter
  @Setter
  private static boolean earliestMessagesTrackerDebugEnabled = false;
}
