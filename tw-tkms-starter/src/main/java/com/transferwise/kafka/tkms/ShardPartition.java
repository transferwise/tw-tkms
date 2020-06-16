package com.transferwise.kafka.tkms;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ShardPartition {

  private int shard;
  private int partition;

  public static ShardPartition of(int shard, int partition) {
    return new ShardPartition().setShard(shard).setPartition(partition);
  }

  public String toString() {
    return "shard " + shard + ", partition " + partition;
  }
}
