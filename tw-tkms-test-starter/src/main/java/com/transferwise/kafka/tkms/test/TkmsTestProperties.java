package com.transferwise.kafka.tkms.test;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TkmsTestProperties {
  private int maxCollectedMessages = 100_000;
}
