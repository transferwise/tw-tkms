package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.config.TkmsProperties;
import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationLevel;
import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationType;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class ProblemNotifier implements IProblemNotifier {

  @Autowired
  private TkmsProperties tkmsProperties;

  @Override
  public void notify(Integer shard, NotificationType notificationType, NotificationLevel defaultLevel, Supplier<String> messageProvider,
      Throwable t) {
    var notificationLevel = shard == null
        ? tkmsProperties.getNotificationLevels().get(notificationType)
        : tkmsProperties.getNotificationLevels(shard, notificationType);

    if (notificationLevel == null) {
      notificationLevel = defaultLevel;
    }

    if (notificationLevel == NotificationLevel.INFO) {
      log.info(messageProvider.get());
    } else if (notificationLevel == NotificationLevel.WARN) {
      log.warn(messageProvider.get(), t);
    } else if (notificationLevel == NotificationLevel.ERROR) {
      log.error(messageProvider.get(), t);
    }
    if (notificationLevel == NotificationLevel.BLOCK) {
      throw new IllegalStateException(messageProvider.get(), t);
    }
  }

  @Override
  public void notify(Integer shard, NotificationType notificationType, NotificationLevel defaultLevel, Supplier<String> messageProvider) {
    notify(shard, notificationType, defaultLevel, messageProvider, null);
  }
}
