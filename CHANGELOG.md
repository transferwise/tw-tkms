# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.0] - 2021-01-06
### Changed
* Default compression is gzip.

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