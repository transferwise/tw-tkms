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
public class DeferredTransactionMessages {

  private static final ThreadLocal<MutableObject<DeferredTransactionMessages>> storage = new ThreadLocal<>();

  Map<TkmsShardPartition, List<TkmsMessage>> shardPartitionMessages = new HashMap<>();

  public static DeferredTransactionMessages get() {
    return getOrCreateHolder().getValue();
  }

  public static DeferredTransactionMessages createAndBind() {
    var deferredTransactionMessages = new DeferredTransactionMessages();
    getOrCreateHolder().setValue(deferredTransactionMessages);
    return deferredTransactionMessages;
  }

  public static void unbind() {
    getOrCreateHolder().setValue(null);
  }

  public static MutableObject<DeferredTransactionMessages> getOrCreateHolder() {
    var holder = storage.get();
    if (holder == null) {
      storage.set(holder = new MutableObject<>());
    }
    return holder;
  }

  public List<TkmsMessage> getMessages(TkmsShardPartition tkmsShardPartition) {
    return shardPartitionMessages.computeIfAbsent(tkmsShardPartition, k -> new ArrayList<>());
  }

}
