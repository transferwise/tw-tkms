# Contributing

The tests are important.

The library has extensive automatic tests coverage, but more will never hurt. However, avoid high maintenance (unit) tests, try to write tests
without mocking anything and do not rely on internal components or their relation.

## ComplexTest

There is also a complex manual test, which needs to be run after larger changes, observing the performance and fault tolerance.

1. Run `docker-compose up` in `demoapp/docker`.
2. Start 2 instances of `com.transferwise.kafka.tkms.demoapp.Application`.
One of them with `node2` Spring profile.
3. Run `com.transferwise.kafka.tkms.ComplexRealTest.complexTest`.
4. Create chaos - crash kafkas, crash apps, crash databases,...
5. Check if test finishes correctly as fast as it did in master.

Repeat the tests for Postgres, by starting `demoapp` with `postgres` Spring profile.

## Raising a PR
Before raising a PR, please ensure that you have updated the [project's version](https://github.com/transferwise/tw-tkms/blob/master/gradle.properties) following [semantic versioning](https://semver.org/spec/v2.0.0.html), and have updated the [CHANGELOG](https://github.com/transferwise/tw-tkms/blob/master/CHANGELOG.md) accordingly.