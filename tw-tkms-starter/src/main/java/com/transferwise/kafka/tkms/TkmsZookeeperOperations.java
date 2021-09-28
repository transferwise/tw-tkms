package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.api.TkmsShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
public class TkmsZookeeperOperations implements ITkmsZookeeperOperations {

  private final Map<TkmsShardPartition, String> lockNodePathMap = new HashMap<>();

  @Autowired
  private TkmsProperties properties;

  @Value("${spring.application.name:}")
  private String applicationName;

  @PostConstruct
  public void init() {
    String groupId = properties.getGroupId();
    if (StringUtils.isEmpty(groupId)) {
      groupId = applicationName;
    }
    if (StringUtils.isEmpty(groupId)) {
      throw new IllegalStateException("`tw-tkms.group-id` nor `spring.application.name` is specified.");
    }
    String prefix = "/tw/tkms/" + groupId + "/";
    String pollerLockPrefix = prefix + "poller/lock/";

    log.info("Using lock pattern of '{}'.", pollerLockPrefix + "{shard}/{partition}");

    for (int s = 0; s < properties.getShardsCount(); s++) {
      for (int p = 0; p < properties.getPartitionsCount(s); p++) {
        lockNodePathMap.put(TkmsShardPartition.of(s, p), pollerLockPrefix + s + "/" + p);
      }
    }
  }

  @Override
  public String getLockNodePath(TkmsShardPartition shardPartition) {
    return lockNodePathMap.get(shardPartition);
  }
}
