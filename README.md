# TransferWise Transactional Kafka Messages Sender

A library for sending Kafka messages according to [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)

* Guarantees messages ordering
  * Messages with same key/partition added in a transaction, will end up in a target Kafka partition in the order they were added.
  * Messages with same key/partition added in two consecutive transactions will end up in a correct Kafka partition in the order they were added.
  * There are no illusionary total/global ordering guarantees, same as for Kafka itself.
* Efficient and high-performing
* Supports both JSON and binary messages
* Supports all Kafka Client features, like headers or forcing specific partitions
* Replaces [tw-tasks-kafka-publisher](https://github.com/transferwise/tw-tasks-executor/tree/master/tw-tasks-kafka-publisher)
* At least once delivery

Do not use this library if
* You do need atomic transactions around your business data and kafka messages.
  * For example
    * you are just converting incoming kafka messages into another topic.
    * only thing your logic does is sending out kafka messages and it does not change the main database. 


You probably would want to start by going over the [main concepts](docs/concepts.md)

First you need to [set it up](docs/setup.md).

Then you can [start using it](docs/usage.md).

Of-course the library can be used in your service's [integration tests](docs/tests.md) 

[Performance considerations](docs/performance.md) can be quite useful to check into.

When there will be [trouble](docs/troubleshooting.md), you will be prepared.

The library is also providing [observability](docs/observability.md).

