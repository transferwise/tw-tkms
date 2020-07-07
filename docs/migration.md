# Migration to TwTkms

With a new service everything is quite simple.

But with already existing service sending messages already by other means, you could mitigate the risk by different steps.

For example.
* Initially, just setup and release the TwTkms.
  * See if Rollbar is empty and metrics get registered and so on. 
* Then just direct a handful of messages or less important topic through it.
* After that move and release other topics, one-by-one if decided so.

## Migrating from tw-tasks-kafka-publisher

> This library is a replacement for tw-tasks-kafka-publisher, the latter is considered as deprecated.

Important here is to not remove the tw-tasks-kafka-publisher with the same release with what you are migrating the last topic.

Instead you need to do one release, allowing all messages already sent by tw-tasks get fully processed. And then after 2 days,
when there is absolutely sure no rollback will be needed, you can finally remove the `tw-tasks-kafka-publisher` dependency and configuration.