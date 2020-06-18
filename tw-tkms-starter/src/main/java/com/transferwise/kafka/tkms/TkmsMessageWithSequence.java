package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsMessage;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TkmsMessageWithSequence {

  private int sequence;
  private TkmsMessage tkmsMessage;
}
