# TransferWise Transactional Kafka Message Sender Documentation
> A library for sending Kafka messages according to the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html).

## Table of Contents
* [About](#about)
* [Main Concepts](concepts.md)
* [Setting the Library Up](setup.md)
* [Using the Library](usage.md)
* [Implementing Integration Tests](testing.md)
* [Performance Considerations](performance.md)
* [Troubleshooting](troubleshooting.md)
* [Observability](observability.md)
* [Metrics Provided by the Library](metrics.md)
* [Migrating to the Library](migration.md)
* [Postgres OLAP/HTAP Workload Issues](postgres_with_long_transactions.md)
* [Database Statistics Bias](database_statistics_bias.md)
* [Contribution Guide](contributing.md)

## About
The library:
* Guarantees messages ordering
    * Messages with the same key/partition added in a transaction, will end up in a target Kafka partition in the order they were added.
    * Messages with the same key/partition added in two consecutive transactions will end up in the correct Kafka partition in the order they were
      added.
    * There are no illusionary global ordering guarantees, the same as for Kafka itself.
* Efficient and high performance
* Supports both JSON and binary messages
* Supports all Kafka Client features, like headers and forcing messages into specific partitions
* Replaces [tw-tasks-kafka-publisher](https://github.com/transferwise/tw-tasks-executor/tree/master/tw-tasks-kafka-publisher)
* At least once delivery
* Supports both Postgres and MariaDb.

Do not use this library

* If you do not need atomic transactions around your business data and Kafka messages.
    * For example:
        * You are just converting incoming Kafka messages into messages for another topic.
        * The only thing your logic does is send out Kafka messages and, it does not change the main database.

You would probably want to start by going over the [main concepts](concepts.md).

First, you need to [set it up](setup.md).

Then you can [start using it](usage.md).

Of course, the library can be used in your service's [integration tests](testing.md)

[Performance considerations](performance.md) can be quite useful to check out.

If there's any [trouble](troubleshooting.md), you will be prepared.

The library also provides [observability](observability.md).

At Wise, we are "enhancing and fixing a flying airplane", so make sure you will not cause any incidents while
[migrating](migration.md) to the library.

Feel free to [contribute](contributing.md) and come and chat in the #tw-task-exec Slack channel.

If you are a Postgres database user, then it is utmost important to be familiar
with [Postgres OLAP/HTAP workloads issues](postgres_with_long_transactions.md)