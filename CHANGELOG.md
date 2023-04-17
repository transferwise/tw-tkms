# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.22.1] - 2023-04-17

### Fixed

* A rare `ConcurrentModificationException` in one of the sqls map.

### Changed

* When Postgres `pg_hint_plan` extension validation fails, we will log out given explain plan and expected plan fragment.
  This would reduce some DBA contacts.

## [0.22.0] - 2023-03-29

### Added

* Supporting multiple data sources out of the box.
  e.g. when shard 0 is using Postgres database and shard 1 another, MariaDb database.

### Changed

* Problem Notifier is used when message is registered without an active transaction.
  The default is still to block, i.e. throw an error, however this can be changed via `notificationLevels` property.

### Migration Guide

#### Active transactions check

`require-transaction-on-messages-registering` configuration option was removed.
If you need to allow message sending without an active transaction, you would now have to disable it via problem notifications.
For example: 
```yaml
tw-tkms:
  notification-levels:
    NO_ACTIVE_TRANSACTION: WARN
```
In any case, it is much cheaper to send non-transactional messages directly and without `tw-tkms`.

#### Multiple databases

If you needed different databases for different shards and used some kind of `CompositeDao` approach, then you would need to remove that
and refactor the code to the approach described in [Recipes](docs/recipes.md#using-different-databases-for-different-shards)

## [0.21.4] - 2023-03-03

### Changed

* When engine independent stats are enabled in MariaDb, we only check if statistics are fixated in those.

* Consequently, in documentation we only ask to fixate engine independent statistics.

### Removed

* All around recommending to fixate `n_distinct` table parameter for Postgres.
  It actually did not seem to work and the correct way is still to rely on `pg_hint_plan` extension.

## [0.21.3] - 2023-02-21

### Fixed

* When Maria index stats query failed, we got another error about transaction being marked rollback only and application startup failed.

## [0.21.2] - 2023-02-09

### Fixed

* Fixed bugs in some metrics.

### Added

* Added `tw_tkms_proxy_poll_in_progress` gauge with `databaseDialect` tag, for being able to distinguish Postgres and Maria services.
* Added `tw_tkms_dao_messages_delete_batches`. `tw_tkms_dao_messages_delete` does not contain batches information anymore, it shows total deleted
  records count.
* Added `tw_tkms_dao_rows_in_engine_independent_table_stats`.

### Removed

* Removed `tw_tkms_stored_message_parsing` metric.

## [0.21.1] - 2023-02-06

### Changed

* Removed `READ_UNCOMMITTED` transactions around Postgres queries.
  For some reason, it slows down the query planner dramatically.
  Besides, the measured benefit from those we only got from MariaDb, when large amount of service are running on the same database server.

## [0.21.0] - 2023-01-31

### Changed

#### Messages polling interval is now more dynamic.

We are pausing for `(pollingInterval * (batchSize - polledRecords) / batchSize)`, instead of just `pollingInteval`

E.g.

- when we poll a full batch, we will not wait at all and start the next cycle immediately.
- when we poll zero records, we will wait the full polling interval.
- when we get half-full batch, we will be waiting 50% from polling interval.

This would allow to reduce the amount of polling queries on Postgres databases for cases where message sending can be latency-tolerant.

For example a Postgres database can have many dead tuples in `Tkms` tables, due to HTAP workloads or autovacuum not being snappy enough to keep those
clean. Every poll query would need to traverse all the dead tuples, even if it would only return couple of records. See
more [here](docs/postgres_with_long_transactions.md).

Essentially it would allow to reduce Postgres database average CPU usage.

The default logic can be overridden by custom implementation of `ITkmsPaceMaker`.

`tw_tkms_proxy_cycle` timer does not include those pauses anymore.
`tw_tkms_proxy_cycle_pause` timer is added to specifically measure those pauses.

When upgrading to this version, it is recommended to take another fresh look of how long polling intervals you would like to use.

#### Index hints for Postgres queries.

It turned out, that the `n_distinct` trick we suggested in [setup](docs/setup.md), did not actually apply in all scenarios. We still had an incident
where delete queries started to do full sequential scans.

With this version, we will be relying on `pg_hint_plan` extension being available. Fortunately RDS is supporting it, meaning it is considered quite
stable.

Some initialization validation routines were added, checking if index hints actually do apply.

Removed the recommendation of setting `autovacuum_vacuum_threshold=100000` for tkms tables. Auto vacuum has its own, database side, `naptime` setting,
to prevent it running in a tight loop. Earlier, that fear of those tight loops were the motivation to recommend it.

#### Configurable delete queries batch sizes.

Services can now configure delete queries batch sizes. This can be useful, when the database is still trying to do sequential scans let's say with
1024 parameters, but would not do it with 256.

This can be done via the `deleteBatchSizes` property.

#### Initialization validations

Added more initialization validations around database performance.

#### MariaDb fixed stats

The stats value recommendations for `stat_value` and `n_rows` was increased to 1,000,000, just in case.

### Docs

Added [Database Statistics Bias](docs/database_statistics_bias.md) to describe the main motivation for this change.

## [0.20.0] - 2023-01-23

### Changed

* Improved graceful shutdown.
  `TransactionalKafkaMessageSender` is now throwing an error when messages are tried to be registered without active transactions.
  It helps to easily detect issues, where database changes from business logic and Kafka messages sending are happening in separate transactions.
  If there is no active transaction, the overhead from `Tkms` does not make any sense.
  The active transaction check can be disabled by setting `requireTransactionOnMessagesRegistering` property to `false`.
  The check can be also by passed by creating an explicit transactions around the message sending code.

## [0.19.1] - 2022-12-05

### Changed

* `@PostConstruct init()` method is now `public`.

## [0.19.0] - 2022-12-02

### Changed

* Allow override of table base name per shard.

## [0.18.0] - 2022-11-28

### Changed

* Make TkmsDao abstract and provide two implementations TkmsMariaDao and TkmsPostgresDao.
* Allow override of database dialect for a shard.

## [0.17.0] - 2022-11-28

### Changed

* Upgraded libraries
* CI is testing the library against multiple Spring versions.

## [0.16.1] - 2022-11-10

### Fixed

* Class cast exception, when KeyHolder value is not a long (e.g. BigInteger). This happens with the latest maria driver on some db schemas.

## [0.16.0] - 2022-10-12

### Added

* Adding Kafka Producer metrics using Micrometer.

## [0.15.0] - 2022-05-28

### Changed

* Update protobuf version from 3.18.0 to 3.20.1 in order to fix CVE-2021-22569.

## [0.14.3] - 2021-12-10

### Changed

* Removed explicit dependency on flyway beans, while making sure it's configured before tkms if provided.

## [0.14.2] - 2021-10-21

### Fixed

* When table and index statistics validation fails, we log it as an error, but continue.

## [0.14.1] - 2021-10-21

### Fixed

* Nullpointer when trying to unregister earliest message tracker metric, even when the system is not enabled.

### Added

* `tableStatsValidationEnabled` property.
  Some older databases may not have enough privileges for those checks yet.
  So while a team waits behind DBAs to add those privileges, they can temporarily turn those checks off.

## [0.14.0] - 2021-09-28

### Changed

* Handling a case, where Postgres database has long running transactions, and those are preventing dead tuples being cleared for tw-tkms tables,
  resulting in massive tw-tkms slow down and database overload.
  More detailed info [here](docs/postgres_with_long_transactions.md).

## [0.13.0] - 2021-05-31

### Changed

* JDK 11+ is required
* Facelift for open-source.

## [0.12.0] - 2021-04-06

### Fixed

* For leader locks, when `tw-tkms.group-id` is missing, we now correctly fall back to `spring.application.name`.
* Partition tag is correctly set for metrics.

### Changed

* Mariadb SELECT query has index hint, in case table stats are wrong and not faked as per documentation.

### Added

* `tw-tkms.min-polling-interval` for globally limiting polling frequency in some environments.

## [0.11.0] - 2021-03-16

### Changed

* Removed redundant configuration parameter of `useCompression`.

## [0.10.0] - 2021-03-01

### Changed

* Default polling timeout was increased from 10 ms to 25 ms.

### Added

* MetricCache is now used to circumvent Micrometer inefficiencies.

## [0.9.1] - 2021-02-12

### Fixed

Buckets count for metric `tw_tkms_dao_poll_all_results_count` are now correctly bounded.

## [0.9.0] - 2021-02-10

### Changed

* Message interceptors are now able to do batch processing.
  This can be useful to allow batch processing techniques and for example to reduce the amount of database transactions.

## [0.8.0] - 2021-01-06

### Changed

* Default compression is gzip.
  It is most appropriate for typical Transferwise messages.

### Added

* A mechanism to force specific migration paths.
  Service owner can specify which version is running in production by `TkmsProperties.Environment.previousVersion`.
  If the version is too old, the service will refuse to start. It can be fixed by doing upgrades to intermediate versions.

* Metrics
  `tw_tkms_dao_serialization_original_size_bytes {shard, partition, algorithm}`
  `tw_tkms_dao_serialization_serialized_size_bytes {shard, partition, algorithm}`
  `tw_tkms_dao_serialization_compression_ratio {shard, partition, algorithm}`

## [0.7.3] - 2021-01-03

### Changed

* Keep the default compression algorithm Snappy, to allow seemless upgrade from 0.6.x

## [0.7.2] - 2020-12-21

### Fixed

* Custom data source providers need to extend ITkmsDataSourceProvider interface instead of concrete class

## [0.7.1] - 2020-12-21

### Fixed

* Correct SLO scale from summary type of metrics.

### Added

* tw_tkms_dao_insert_invalid_generated_keys_count metric, to measure, if all messages get inserted and their primary keys returned.

## [0.7.0] - 2020-12-18

### Added

* LZ4 and Gzip compressions.

## [0.6.2] - 2020-12-17

### Fixed

* Registering a metric for failed kafka send resulted in error because of wrong set of tags being used.

## [0.6.1] - 2020-11-13

### Fixed

* Control was not always yielded for proxy leader.
  On some errors from micrometer we ran into a situation where no pod was proxying the messages
  and no errors were reported.

## [0.6.0] - 2020-11-09

### Changed

* Default compression algorithm is Snappy.
  Only upgrade from 0.5.0 is supported.
  Upgrading directly from older version creates a processing pause until all service nodes have the new version running.

## [0.5.0] - 2020-11-06

### Removed

* Gzip decompressor.
  Only upgrade from 0.3 and from 0.4 is supported. Upgrading directly from older version creates a processing pause
  until all service nodes have the new version running.

### Fixed

* Memory allocation rate considerably reduced.

### Fixed

* The Snappy compressor was doing crazy memory allocations. Now we use reusable byte buffers instead.
  This is achieved by moving away from Airlift's aircompression library and using the same library Kafka client is using:
  Xerial Snappy.

## [0.4.0] - 2020-10-21

### Added

* Fixed an issue where shard kafka properties did not overwrite default kafka properties.
* Upgraded libs.

## [0.3.2] - 2020-09-17

### Added

* We are estimating a message length during registration and rejecting if it is too large.

## [0.3.1] - 2020-09-14

### Fixed

* Optimized the batching of multiple delete operations.

## [0.3.0] - 2020-07-12

### Added

* Using Snappy compression as default, otherwise the upgrade process will create a messages processing pause.
  Only upgrade from 0.2.x is supported. If you upgrade directly from 0.1.x, a processing pause can happen.

## [0.2.1] - 2020-07-10

### Added

* Using Zlib compression as default, otherwise the upgrade process will create a messages processing pause.

## [0.2.0] - 2020-07-10

### Added

* Better defaults for sending out messages in a fast and ordered way.
* Using pure-java Snappy compression implementation instead of native Zlib.

## [0.1.3] - 2020-07-09

### Added

* Special handling for Kafka's retryable exceptions.

## [0.1.2] - 2020-07-08

### Changed

* `tw_tkms_proxy_cycle` metric has `pollResult` tag, which indicates if it was "an empty cycle" or not.

## [0.1.1] - 2020-07-07

### Added

* First public version.
