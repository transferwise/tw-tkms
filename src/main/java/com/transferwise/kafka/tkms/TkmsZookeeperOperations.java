package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.ExceptionUtils;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TkmsZookeeperOperations implements ITkmsZookeeperOperations {

  private final Map<Integer, String> lockNodePathMap = new HashMap<>();

  @Autowired
  private TkmsProperties properties;

  @PostConstruct
  public void init() {
    ExceptionUtils.doUnchecked(() -> {
      String prefix = "/tw/tkms/" + properties.getGroupId() + "/";
      String pollerLockPrefix = prefix + "poller/lock/";

      log.info("Using '{}' Zookeeper nodes.", pollerLockPrefix + "{shard}");

      for (int i = 0; i < properties.getShardsCount(); i++) {
        lockNodePathMap.put(i, pollerLockPrefix + i);
      }
    });
  }

  @Override
  public String getLockNodePath(int shard) {
    return lockNodePathMap.get(shard);
  }
}
