# TransferWise Transactional Kafka Messages Sender

A library for sending Kafka messages according to [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)

- Guarantees messages ordering
- Efficient and high-performing
- Supports both JSON and binary messages
- Supports all Kafka Client features, like headers or forcing specific partitions
- Replaces [tw-tasks-kafka-publisher](https://github.com/transferwise/tw-tasks-executor/tree/master/tw-tasks-kafka-publisher)
- At least once delivery

## Setup

We are assuming you are using Spring Boot, at least version 2.1.

As usual, we can define it through [tw-dependency](https://github.com/transferwise/tw-dependencies)
```groovy
implementation platform("com.transferwise.common:tw-dependencies:${twDependenciesVersion}")
implementation 'com.transferwise.kafka:tw-tkms-starter'
```

```java
@Autowired
private ITransactionalKafkaMessageSender transactionalKafkaMessageSender;
```

You may also want to use following to create json messages
```java
@Autowired
private ITkmsMessageFactory tkmsMessageFactory;
```

And now you can start sending messages
```java
transactionalKafkaMessageSender.sendMessage(tkmsMessageFactory.createJsonMessage(value).setTopic(topic).setKey(key));
```

Configuration can be tweaked according to `com.transferwise.kafka.tkms.config.TkmsProperties`. Usually there is no need to change defaults.

However, if you are using Postgres database, you need to change
```yaml
tw-tkms.database-dialect: POSTGRES
```

Of-course you need database tables as well.
For each shard & partition combination, you need a table in a form of `outgoing_message_<shard>_<partition>`.

#### Set up database tables for MariaDb

```mariadb
CREATE TABLE outgoing_message_0_0(
              id BIGINT AUTO_INCREMENT PRIMARY KEY,
              message MEDIUMBLOB NOT NULL)
              stats_persistent=1, stats_auto_recalc=0 ENGINE=InnoDB;

update mysql.innodb_index_stats set stat_value=1000000 where table_name = "outgoing_message_0_0" and stat_description="id";
update mysql.innodb_table_stats set n_rows=1000000 where table_name like "outgoing_message_0_0";
flush table outgoing_message_0_0;
```

Fixing tables statistics in place like that is absolutely crucial. As those command require specific permissions, you may need
help from DBAs.

Also, ask DBAs to set [innodb_autoinc_lock_mode](https://mariadb.com/docs/reference/es/system-variables/innodb_autoinc_lock_mode/) to 2.

#### Set up database tables for Postgres

TBD TBD TBD

```postgresql
CREATE TABLE outgoing_message_0_0(
              id BIGSERIAL PRIMARY KEY,
              message BYTEA NOT NULL);
```

TBD TBD TBD

TDB : Statistics, auto vacuum settings, ...

#### Advanced setup
If you have multiple dataSources, you can use `Tkms` annotation to mark a datasource to be used.
Alternatively you can provide `ITkmsDataSourceProvider` implementation bean.

## Test

`TwTkms` also provides helpers for conducting end-to-end integration tests.

For that, you can add a dependency.
```groovy
testImplementation 'com.transferwise.kafka:tw-tkms-test-starter'
```

You will get the following beans.
```java
@Autowired
protected ITkmsSentMessagesCollector tkmsSentMessagesCollector;
@Autowired
protected ITransactionalKafkaMessageSender tkmsTransactionalKafkaMessageSender;
@Autowired
protected ITkmsTestDao tkmsTestDao
```

Usually you are using only `ITkmsSentMessagesCollector` though.

Make sure, you are cleaning the memory regularly. For example before each test:
```java
registeredMessagesCollector.clear()
tkmsSentMessagesCollector.clear()
```

## Concepts and algorithm

#### Shards
First we can have multiple **shards**.  

Shards can be used for having specific configuration for some messages or most commonly for separating for example high latency
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
 
#### Partitions
Every shard can have a number of partitions. 

Do not confuse them with Kafka topics partitions, they are completely separate ones.

Performance tests show, that to get the best throughput, relying on more than one messages table, gives benefits, though
with diminishing returns. By adding partitions, the `tw-tkms` will proxy messages through multiple tables, for example for 
shard #0, through outgoing_message_0_0, outgoing_messages_0_1, outgoing_messages_0_2, ...

It is highly likely that you do not need more that 1 partitions.

#### Algorithm

When message sending has been requested, a shard and partition will be determined for that message.

Message will be added into a table, based on that shard and partition, for example into `outgoing_message_1_0`.
This is usually done in a business transaction, started outside of `tw-tkms`. So if that business transaction gets 
committed, the message will get persisted and if it will be rolled back, there will be no message registered nor sent.

For every shard and partition combination, we have a thread constantly polling a table for new messages. Those threads are 
coordinated by using Zookeeper.

If a thread see new messages in that table, it will send them out using `KafkaProducer` api.

If KafkaProducer acknowledges the message, it will immediately get deleted from the table.

The difference with other similar libraries is, that we don't have any state machine nor do we use any indexes, except the primary key.
This would make the storage layer as efficient as possible, reducing iops, compared to alternatives.

## Performance considerations

The total throughput will depend on numerous factors - 
* message content size
* database performance and latency
* number of shards and partition used
* average number of messages per transaction
* network throughput
* and others...

The only meaningful recommendation here is to monitor and tune. 

On a local laptop, where Kafka, MariaDb and service nodes are running, 50,000 messages/s has been achieved. In a production
where processes are running in their own servers, the number can be considerably higher.

The main bottlenecks in local tests are around how many bytes we are pumping through different things, i.e. message size is the main factor.
That is why we do compress messages with ZLIB by default - application CPU is not an issue.

Notice, that you are not using your database only for tw-tkms, so you still have to be careful about how many messages you are registering.
Most likely you would want to have additional concurrency controls, like rate limiters or bulkheads in place, which you can
tune according to your monitoring results.

You may also want to consider sharding. For example, if you would like to have 1s latency for some topic, but other topic
gets 1 mln messages inserted at once, it could take 20s for that topic to have a chance.

Consider batching of messages. The most beneficial is to make sure, that there is a single transaction around multiple messages sending.

There is also an interface to send multiple messages at once, which will rely on database batch-insert, but that does not give so many benefits
unless you really want to go crazy on latency.

As usual with databases, batching is good, but do not create too large transactions. They can start affecting other aspects of your database,
for example replication lag, or even preventing fast cleaning of tuples.

#### Latency considerations
Latency is depending on many factors as well.

TBD TBD TBD

## Failure scenarios and troubleshooting

Not that those are likely, but they can give you more insight how things work internally.

### Topic does not exist

When you are registering a message, tw-tkms will check if a topic exists and is available for writing.
 
However, if that topic is deleted afterwards, before the Proxy can send a message out, it will block the processing of that shard-partition entirely.

So make sure, you are not suddenly deleting topics :). If that happens, you can ask Messaging team to put that topic back or 
you need to ask DBAs to delete a specific row from a database (the storage id will be in error message). 

### Corrupted message in a database

Any corrupted message in a shard-partition table can halt processing of that shard-partition.

Probably could happen only when a new tw-tkms version is released without full backward-compatibility.

To mitigate that risk, make sure you upgrade the library at least once per quarter.

If you don't have a DBA in hand, you can temporarily send messages to another shard.

Or you can implement an `ITkmsMessageInterceptor` which will save the message to another table (DLQ) and return `Result.DISCARD`


## Observability

[Metrics](docs/metrics.md)

[Dashboard](https://dashboards.tw.ee/d/6ipjjmmMk/tw-tkms?orgId=1&refresh=30s) 

