package com.transferwise.kafka.tkms;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.api.ShardPartition;
import com.transferwise.kafka.tkms.config.TkmsProperties;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;

@Slf4j
public class TkmsZookeeperOperations implements ITkmsZookeeperOperations {

  private final Map<ShardPartition, String> lockNodePathMap = new HashMap<>();

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
    ExceptionUtils.doUnchecked(() -> {
      String prefix = "/tw/tkms/" + properties.getGroupId() + "/";
      String pollerLockPrefix = prefix + "poller/lock/";

      log.info("Using lock pattern of '{}'.", pollerLockPrefix + "{shard}/{partition}");

      for (int s = 0; s < properties.getShardsCount(); s++) {
        for (int p = 0; p < properties.getPartitionsCount(s); p++) {
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
