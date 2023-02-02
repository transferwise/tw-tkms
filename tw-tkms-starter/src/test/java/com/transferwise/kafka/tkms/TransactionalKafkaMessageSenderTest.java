package com.transferwise.kafka.tkms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class TransactionalKafkaMessageSenderTest {

  @Test
  void deleteBatchSizesAreCorrectlyValidated() {
    var sender = new TransactionalKafkaMessageSender();

    sender.validateDeleteBatchSize(0, List.of(256, 4, 1), "test");

    var e = assertThrows(IllegalStateException.class, () -> sender.validateDeleteBatchSize(0, List.of(1, 2, 1), "test"));
    assertEquals("Invalid delete batch sizes provided for 'test', 1<=2.", e.getMessage());

    e = assertThrows(IllegalStateException.class, () -> sender.validateDeleteBatchSize(1, List.of(-2, -3, 1), "test"));
    assertEquals("Invalid delete batch sizes provided for 'test', -2<1.", e.getMessage());

    e = assertThrows(IllegalStateException.class, () -> sender.validateDeleteBatchSize(1, List.of(4, 2), "test"));
    assertEquals("Invalid delete batch sizes provided for 'test', last element has to be 1.", e.getMessage());
  }
}
