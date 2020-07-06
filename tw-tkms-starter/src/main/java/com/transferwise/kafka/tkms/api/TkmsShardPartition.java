package com.transferwise.kafka.tkms.api;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TkmsShardPartition {
  
  private int shard;
  private int partition;

  public static TkmsShardPartition of(int shard, int partition) {
    return new TkmsShardPartition().setShard(shard).setPartition(partition);
  }

  public String toString() {
    return "shard " + shard + ", partition " + partition;
  }
}
