# TransferWise Transactional Kafka Message Sender
![Apache 2](https://img.shields.io/hexpm/l/plug.svg)
![Java 11](https://img.shields.io/badge/Java-11-blue.svg)
![Maven Central](https://badgen.net/maven/v/maven-central/com.transferwise.kafka/tw-tkms-starter)
[![Owners](https://img.shields.io/badge/team-AppEng-blueviolet.svg?logo=wise)](https://transferwise.atlassian.net/wiki/spaces/EKB/pages/2520812116/Application+Engineering+Team) [![Slack](https://img.shields.io/badge/slack-tw--task--exec-blue.svg?logo=slack)](https://wise.slack.com/archives/C7P9L0B6Z)
> Use the `@application-engineering-on-call` handle on Slack for help.
---

A library for sending Kafka messages according to the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)

* Guarantees messages ordering
  * Messages with the same key/partition added in a transaction, will end up in a target Kafka partition in the order they were added.
  * Messages with the same key/partition added in two consecutive transactions will end up in the correct Kafka partition in the order they were added.
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
    * The only thing your logic does is send out Kafka messages and it does not change the main database.

You would probably want to start by going over the [main concepts](docs/concepts.md).

First, you need to [set it up](docs/setup.md).

Then you can [start using it](docs/usage.md).

Of course, the library can be used in your service's [integration tests](docs/testing.md)

[Performance considerations](docs/performance.md) can be quite useful to check out.

If there's any [trouble](docs/troubleshooting.md), you will be prepared.

The library also provides [observability](docs/observability.md).

At TransferWise we are "enhancing and fixing a flying airplane", so make sure you will not cause any incidents while
[migrating](docs/migration.md) to the library.

Feel free to [contribute](docs/contributing.md) and come and chat in the #tw-task-exec Slack channel.

## License
Copyright 2021 TransferWise Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
