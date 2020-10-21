# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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