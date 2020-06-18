package com.transferwise.kafka.tkms.api;

import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.Singular;
import lombok.experimental.Accessors;

public interface ITransactionalKafkaMessageSender {

  SendMessageResult sendMessage(TkmsMessage message);

  @Data
  @Accessors(chain = true)
  class SendMessageResult {

    private Long storageId;
    private ShardPartition shardPartition;
  }

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

    @Singular
    @NotNull
    @NotEmpty
    private List<SendMessageResult> results = new ArrayList<>();
  }
}
