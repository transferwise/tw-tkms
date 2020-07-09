# Contributing

The tests are important.

The library has extensive automatic tests coverage, but more will never hurt. However, avoid high maintenance (unit) tests, try to write tests
without mocking anything and do not rely on internal components nor their relation.

## ComplexTest

There is also a complex manual test, which needs to be run after larger changes, observing the performance and fault tolerance.

1. Run `docker-compose up` in `demoapp/docker`.
2. Start 2 instances of `com.transferwise.kafka.tkms.demoapp.Application`.
One of them with `node2` Spring profile.
3. Run `com.transferwise.kafka.tkms.demoapp.Application`.
4. Create chaos - crash kafkas, crash apps, crash databases,...
5. Check if test finishes correctly as fast as it did in master.

Repeat the tests for Postgres, by starting `demoapp` with `postgres` Spring profile.