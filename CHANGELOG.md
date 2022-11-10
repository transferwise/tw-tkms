# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
