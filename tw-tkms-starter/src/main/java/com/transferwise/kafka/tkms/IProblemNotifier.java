package com.transferwise.kafka.tkms;

import com.transferwise.kafka.tkms.config.TkmsProperties.NotificationLevel;
import java.util.function.Supplier;

/**
 * The main idea here is to allow service teams to "tweak" the notifications level/action of specific problems.
 */
public interface IProblemNotifier {

  void notify(Integer shard, String notificationType, NotificationLevel defaultLevel, Supplier<String> messageProvider);

  void notify(Integer shard, String notificationType, NotificationLevel defaultLevel, Supplier<String> messageProvider,
      Throwable t);
}
