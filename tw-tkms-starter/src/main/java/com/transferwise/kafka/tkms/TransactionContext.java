package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsMessage;
import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.mutable.MutableObject;

@Data
@Accessors(chain = true)
@NotThreadSafe
public class TransactionContext {

  protected static final ThreadLocal<MutableObject<TransactionContext>> storage = new ThreadLocal<>();

  private final Map<TkmsShardPartition, ShardPartitionMessages> shardPartitionMessagesMap = new HashMap<>();

  private long registeredMessagesCount;

  public static TransactionContext get() {
    return getOrCreateHolder().getValue();
  }

  public static TransactionContext createAndBind() {
    var deferredTransactionMessages = new TransactionContext();
    getOrCreateHolder().setValue(deferredTransactionMessages);
    return deferredTransactionMessages;
  }

  public static void unbind() {
    getOrCreateHolder().setValue(null);
  }

  public static MutableObject<TransactionContext> getOrCreateHolder() {
    var holder = storage.get();
    if (holder == null) {
      storage.set(holder = new MutableObject<>());
    }
    return holder;
  }

  public void countMessage() {
    registeredMessagesCount++;
  }

  public ShardPartitionMessages getShardPartitionMessages(TkmsShardPartition tkmsShardPartition) {
    return shardPartitionMessagesMap.computeIfAbsent(tkmsShardPartition, k -> new ShardPartitionMessages());
  }

  @Data
  @Accessors(chain = true)
  public static class ShardPartitionMessages {

    private Mode mode = null;

    List<TkmsMessage> messages = new ArrayList<>();
  }

  public enum Mode {
    DEFERRED, NOT_DEFERRED;
  }

}
