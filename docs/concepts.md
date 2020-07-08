# Concepts and algorithm

## Shards
  
Shards can be used for completely separating processing of some messages from others.    

Also, every shard can have a specialised configuration, most suitable for specific messages. 
 
For example, a common use case will be separating a small subset of messages with low-latency requirements from the rest.

This could be accomplished by defining:
```java
public static final int DEFAULT_SHARD = 0;
public static final int LOW_LATENCY_SHARD = 1;
``` 

And then sending a message:
```java
transactionalKafkaMessageSender.sendMessage(message.setShard(LOW_LATENCY_SHARD));
```

Usually you do not need to use shards, but by default we are defining 2. The idea is to have one spare shard ready for unforeseen situations.

By default all messages will go to shard #0, but if the situation calls for it, the second shard is ready for use
without needing to create new tables. One example would be an incident caused by all messages being affected by
latencies related to only a subset of the messages.
 
>Notice that the messages' order is guaranteed only in the same shard. When ordering is important, make sure to not send
>the same entity's messages into the same topic using different shards.
 
## Partitions
Every shard can have a number of partitions. 

> Do not confuse them with Kafka partitions.

Performance tests show that using multiple messages tables increases throughput, although with diminishing
returns. By adding partitions, TwTkms will proxy messages through multiple tables. For example, with 3 partitions,
messages to shard #0 would be proxied through tables `outgoing_message_0_0`, `outgoing_messages_0_1`, and `outgoing_messages_0_2`.

It is highly likely that you do not need more than 1 partition.

## Algorithm

Simplified, the messages go through the following steps.

1. Gets inserted into a table from business transactions and threads.
2. Polled from that table in a separate thread. In the documentation, this component/thread is often referred as "Proxy" or "Kafka Proxy".
   
   2.1. Sent to the Kafka, using `kafka-clients` library.

   2.2. Deleted from the table.

When message sending has been requested, a shard and partition will be determined for that message.

Message will be added into a table, based on that shard and partition, for example into `outgoing_message_1_0`.
This is usually done in a business transaction, started outside of TwTkms. If that business transaction gets 
committed, the message will get persisted and later sent. If the business transaction is rolled back, the message is
neither persisted nor sent.

For every shard and partition combination, we have a thread constantly polling a table for new messages. Those threads are 
coordinated by using Zookeeper.

If a thread "sees" new messages in that table, it will send them out using Kafka client's `KafkaProducer` api.

If KafkaProducer acknowledges the message, it will immediately get deleted from that table.

>The difference with other similar libraries is, that we don't have any state machine nor do we use any indexes, except the primary key one.
>This makes the storage layer as efficient as possible, reducing iops.
