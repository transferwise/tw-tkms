package com.transferwise.kafka.tkms;

import lombok.Setter;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Assertions {

  @Setter
  private static int level = 0;

  public static boolean isEnabled() {
    return level > -1;
  }

  public static boolean isLevel1() {
    return level >= 1;
  }

  public static void assertAlgorithm(boolean result, String message) {
    if (!result) {
      throw new AlgorithmErrorException(message);
    }
  }

}
