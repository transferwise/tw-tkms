package com.transferwise.kafka.tkms.api;

import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.config.TkmsProperties.Mdc;
import io.micrometer.core.instrument.Tag;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.slf4j.MDC;

@Value
@EqualsAndHashCode(of = {"shard", "partition"})
public class TkmsShardPartition {

  private int shard;
  private int partition;
  private Tag micrometerShardTag;
  private Tag micrometerPartitionTag;
  private String stringPresentation;

  public TkmsShardPartition(int shard, int partition) {
    this.shard = shard;
    this.partition = partition;
    this.micrometerShardTag = Tag.of("shard", String.valueOf(shard));
    this.micrometerPartitionTag = Tag.of("partition", String.valueOf(partition));
    this.stringPresentation = "shard " + shard + ", partition " + partition;
  }

  public String toString() {
    return stringPresentation;
  }

  public void putIntoMdc() {
    MDC.put(mdc.getShardKey(), String.valueOf(getShard()));
    MDC.put(mdc.getPartitionKey(), String.valueOf(getPartition()));
  }

  public void removeFromMdc() {
    MDC.remove(mdc.getShardKey());
    MDC.remove(mdc.getPartitionKey());
  }

  public static TkmsShardPartition of(int shard, int partition) {
    return shards[shard].partitions[partition];
  }

  /**
   * Very frequently used object, so we will use "object pooling" instead of creating those instances over and over again.
   */
  public static void init(TkmsProperties tkmsProperties) {
    mdc = tkmsProperties.getMdc();

    shards = new TkmsShard[tkmsProperties.getShardsCount()];
    for (int s = 0; s < shards.length; s++) {
      int partitionsCount = tkmsProperties.getPartitionsCount(s);
      TkmsShard shard = shards[s] = new TkmsShard(partitionsCount);
      shard.partitions = new TkmsShardPartition[partitionsCount];
      for (int p = 0; p < partitionsCount; p++) {
        shard.partitions[p] = new TkmsShardPartition(s, p);
      }
    }
  }

  private static TkmsShard[] shards;

  private static Mdc mdc;

  private static class TkmsShard {

    TkmsShardPartition[] partitions;

    private TkmsShard(int partitionsCount) {
      partitions = new TkmsShardPartition[partitionsCount];
    }
  }
}

