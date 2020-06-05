package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TkmsZookeeperOperations implements ITkmsZookeeperOperations {

  private final Map<ShardPartition, String> lockNodePathMap = new HashMap<>();

  @Autowired
  private TkmsProperties properties;

  @PostConstruct
  public void init() {
    ExceptionUtils.doUnchecked(() -> {
      String prefix = "/tw/tkms/" + properties.getGroupId() + "/";
      String pollerLockPrefix = prefix + "poller/lock/";

      log.info("Using lock pattern of '{}'.", pollerLockPrefix + "{shard}/{partition}");

      for (int s = 0; s < properties.getShardsCount(); s++) {
        for (int p = 0; p < properties.getPartitionsCount(); p++) {
          lockNodePathMap.put(ShardPartition.of(s, p), pollerLockPrefix + s + "/" + p);
        }
      }
    });
  }

  @Override
  public String getLockNodePath(ShardPartition shardPartition) {
    return lockNodePathMap.get(shardPartition);
  }
}
