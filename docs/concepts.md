# Concepts and algorithm

## Shards
  
Shards can be used for having specific configuration for some messages or most commonly for separating for example latency
tolerating messages and messages needing very low latencies.

For example one could define
```java
public static final int DEFAULT_SHARD = 0;
public static final int LOW_LATENCY_SHARD = 1;
``` 

And then send a message
```java
transactionalKafkaMessageSender.sendMessage(message.setShard(LOW_LATENCY_SHARD));
```

Usually you don't need to use shards, but by default we are defining 2. By the default all messages will go to #0, but
if someone figures out during an incident, that some messages are actually affecting other messages latencies, you can
 quickly start using it without needing DBAs help.
 
## Partitions
Every shard can have a number of partitions. 

>> Do not confuse them with Kafka partitions.

Performance tests show, that to get the best throughput, relying on more than one messages table, gives benefits, though
with diminishing returns. By adding partitions, the `tw-tkms` will proxy messages through multiple tables, for example for 
shard #0, through outgoing_message_0_0, outgoing_messages_0_1, outgoing_messages_0_2, ...

It is highly likely that you do not need more that 1 partitions.

## Algorithm

So basically the message goes through the following steps.

1. Gets inserted into a table.
2. Polled from that table.
3. Sent to the Kafka.
4. Deleted from it.

When message sending has been requested, a shard and partition will be determined for that message.

Message will be added into a table, based on that shard and partition, for example into `outgoing_message_1_0`.
This is usually done in a business transaction, started outside of TwTkms. So if that business transaction gets 
committed, the message will get persisted and if it will be rolled back, there will be no message registered nor sent.

For every shard and partition combination, we have a thread constantly polling a table for new messages. Those threads are 
coordinated by using Zookeeper.

If a thread sees new messages in that table, it will send them out using Kafka client's `KafkaProducer` api.

If KafkaProducer acknowledges the message, it will immediately get deleted from that table.

>>The difference with other similar libraries is, that we don't have any state machine nor do we use any indexes, except the primary key one.
>>This would make the storage layer as efficient as possible, reducing iops.
