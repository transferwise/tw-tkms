package com.transferwise.kafka.tkms.api;

import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ITransactionalKafkaMessageSender {

  /**
   * Registers a message to be sent out.
   */
  SendMessageResult sendMessage(TkmsMessage message);

  @Data
  @Accessors(chain = true)
  class SendMessageResult {

    /**
     * The id in the database table.
     */
    private Long storageId;
    /**
     * Shard-partition message was put into.
     *
     * <p>You can determine the table's name by that.
     */
    private TkmsShardPartition shardPartition;
  }

  /**
   * Batch variant for {@link ITransactionalKafkaMessageSender#sendMessage(com.transferwise.kafka.tkms.api.TkmsMessage)}
   *
   * <p>Can be useful, when you have latency concerns from sequential processes.
   * For example when importing 50,000 bank transactions.
   */
  SendMessagesResult sendMessages(SendMessagesRequest request);

  @Data
  @Accessors(chain = true)
  class SendMessagesRequest {

    @NotNull
    @NotEmpty
    private List<TkmsMessage> tkmsMessages = new ArrayList<>();

    public SendMessagesRequest addTkmsMessage(TkmsMessage tkmsMessage) {
      tkmsMessages.add(tkmsMessage);
      return this;
    }
  }

  @Data
  @Accessors(chain = true)
  class SendMessagesResult {

    @NotNull
    @NotEmpty
    private List<SendMessageResult> results = new ArrayList<>();
  }
}
