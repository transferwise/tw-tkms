# Random ideas, brainstorm

3. Avoid engineers creating tables manually. Provide at least Flyway auto configuration.

5. Add hierarchical Valid annotations for TkmsProperties.

6. It turns out, than in the polling cycle, the sending messages to the Kafka can be by far the slowest step,
looking at 95th percentiles. We need to figure out, how to overcome this.
* producer per partition?
* proper producer configuration - large enough batch sizes, lingering, in flight requests count etc.

7. When earliest visible messages is enabled, but we have left over messages, we currently just log out that they exist.

This is not sufficient.

We should provide counter about how many messages there are like that. So teams can have metric based alerts.

We should also provide a solution to overcome that situation automatically. One option would be to provide a hybrid poll
solution, where from configurable interval we would poll all the messages/tuples.

8. `validateIndexHintsExtension` nees to log the explain plans on failure. So we don't have to ask that info from DBAs.

9. Investigate timeouts on partition leader change.

Sometimes people get errors like:  org.apache.kafka.common.errors.TimeoutException: Expiring 1 record(s) for RulesFeatureAssembler.in.FeatureGatheringResponse-13:10004 ms has passed since batch creation


Tamás
i think tw-tkms is being a bit too restrictive with the delivery timeout config: https://github.com/transferwise/tw-tkms/blob/15b992aa78f397840b0603970fd10ff90a1b6[…]m/transferwise/kafka/tkms/config/TkmsKafkaProducerProvider.java
it is set to 10 secs, whereas the kafka default is 2 minutes: https://kafka.apache.org/documentation/#producerconfigs_delivery.timeout.ms
it seems that the 10 sec is not always enough to handle a partition leadership change - maybe with a bit larger value automatic retries could recover the message sending and we wouldn’t get the TimeoutException
any thoughts about this

Levani
Problem here is not a leadership change on the broker, which is usually fast, but that producers have an old metadata cache and this exception forces the producer to refresh the metadata about the brokers. It’s possible to configure the clients to have more frequent metedata refresh (metadata.max.age config), but there will always be some noise around this unless producer internal mechanisms of detecting the stale matadata changes.
I’m not aware of any KIPs that addresses this, there’s strictly uniform partitioner proposal, but it solves a bit different problem.
It’s possible to have our own partitioner and producer interceptor to avoid downed brokers (that’s what Twitter did when they adopted Kafka), maybe worth to experiment around it at some point. I think we will get more ideas about this at Kafka Summit this year :slightly_smiling_face:


