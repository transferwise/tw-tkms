# Setup

We are assuming you are using Spring Boot, at least version 2.5.

First ensure that you have the `mavenCentral` repository available in your Gradle buildscript:

```yaml
repositories {
  mavenCentral()
}
```

Then, declare the tw-twks library as a dependency in your Gradle buildscript:

```groovy
dependencies {
    implementation 'com.transferwise.kafka:tw-tkms-starter:<VERSION>'
}
```

> Replace `<VERSION>` with the version of the library you wish to use.

Configuration can be tweaked according to `com.transferwise.kafka.tkms.config.TkmsProperties`. Usually there is no need to change the defaults.

Minimum required configuration is:

```
tw-tkms:
  database-dialect: POSTGRES # only required if using Postgres, MariaDb is the default
  kafka.bootstrap.servers: ${ENV_SECURE_KAFKA_BOOTSTRAP_SERVERS}
  environment:
    previous-version-at-least: ${LIB_VERSION} # use current lib version for a new integration
```

Of course, you need to create tables in the database as well.

For each shard & partition combination, you need a table in a form of `outgoing_message_<shard>_<partition>`.

> If you are using Flyway, it can be achieved programmatically. For inspiration, check the `V1__Init` classes in this repo.

The tricky part is that those tables have to be created in exact way, to trick the database statistics not to consider
the table as empty and thus forcing it to rely more on primary key indexes. This is crucial to have minimum locking between the threads and
optimal performance even in cases where those tables will suddenly accumulate large number of messages.

## MariaDb

<!-- @formatter:off -->
```mariadb
CREATE TABLE outgoing_message_0_0 (
              id BIGINT AUTO_INCREMENT PRIMARY KEY,
              message MEDIUMBLOB NOT NULL)
              stats_persistent=1, stats_auto_recalc=0 ENGINE=InnoDB;

-- Set engine independent stats.
insert into mysql.table_stats (db_name, table_name, cardinality) values(DATABASE(), "outgoing_message_0_0", 1000000)
                                                                 on duplicate key update cardinality=1000000;

-- Apply the stats to be used for next queries.
flush tables;
```
<!-- @formatter:on -->

> Make sure you **never** run ANALYZE on those tables as it will overwrite those stats, and you will end up with database going crazy on certain
> situations.

> We are setting engine independent stats as well, so that a simple ANALYZE would not mess things up.

As some of those commands require specific permissions, you most likely will need some help from DBAs.

Also, it is beneficial (but not crucial) to
set [innodb_autoinc_lock_mode](https://mariadb.com/docs/reference/es/system-variables/innodb_autoinc_lock_mode/) to 2.

## Ancient MariaDb versions or where engine independent stats are disabled

If we can not rely on engine independent statistics, we can still rely on innodb engine statistics as following.

<!-- @formatter:off -->
```mariadb
-- Set engine stats.
update mysql.innodb_index_stats set stat_value=1000000 where table_name = "outgoing_message_0_0" and stat_description="id";
update mysql.innodb_table_stats set n_rows=1000000 where table_name like "outgoing_message_0_0";
```
<!-- @formatter:on -->

But notice, that anyone running `ANALYZE` on those tables, will rewrite statistics entries, and you would have to replay the statements above.

### Troubleshooting
If you run into issues when testing locally or on CI, your flyway user may be missing the required permissions to run the migration script.

#### Local Development

1. Add a sql script named `db-setup.sql` into the directory where your `docker-compose.yml` file is located. Use the following script, where `test` is the username of your mysql user.

<!-- @formatter:off -->
```mariadb
GRANT ALL PRIVILEGES ON *.* TO 'test'@'%';
GRANT SELECT, UPDATE ON mysql.innodb_index_stats to 'test'@'%';
GRANT SELECT, UPDATE ON mysql.innodb_table_stats to 'test'@'%';
GRANT reload ON *.* TO 'test'@'%';
```
<!-- @formatter:on -->

2. Add the following into your `docker-compose.yml` file, under your MariaDb image:

<!-- @formatter:off -->
```yml
volumes:
  - ./db-setup.sql:/docker-entrypoint-initdb.d/db-setup.sql
```
<!-- @formatter:on -->

#### CI
1. Ensure your test configuration uses environment variables to set the credentials for Flyway. For example:
<!-- @formatter:off -->
```yml
maria-db:
  flyway:
    username: "${ENV_DB_FLYWAY_USERNAME:test}"
    password: "${ENV_DB_FLYWAY_PASSWORD:test}"
```

2. Inject environment variables in your `build.gradle` file to use root credentials for Flyway. For example:
<!-- @formatter:off -->
```
workflow {
    configService {
        dockerServices = [
                standardDockerServices.mariaDb(),
                standardDockerServices.zookeeper(),
                standardDockerServices.kafkaZookeeper(),
                standardDockerServices.kafka(),
        ]
        env = [ENV_DB_FLYWAY_USERNAME: "root", ENV_DB_FLYWAY_PASSWORD: "admin"]
    }
}
```
<!-- @formatter:on -->


## Postgres

It is utmost important to have [pg_hint_plan](https://github.com/ossc-db/pg_hint_plan) extension installed in Postgres.

<!-- @formatter:off -->
```postgresql
CREATE TABLE outgoing_message_0_0 (
  id BIGSERIAL PRIMARY KEY,
  message BYTEA NOT NULL
) WITH (autovacuum_analyze_threshold=2000000000, toast_tuple_target=8160);
```
<!-- @formatter:on -->

> toast_tuple_target - we should avoid getting payloads to TOAST. Payloads will get deleted quickly anyway, so inlining as much as possible would not
> hurt.

Postgres tries to compress the TOAST entry, when it is large enough (by default 2kb). But because `tw-tkms` already applies compression,
it will be wasted effort and resources.

So let's turn that compression off.

<!-- @formatter:off -->
```postgresql
ALTER TABLE outgoing_message_0_0 ALTER COLUMN message SET STORAGE EXTERNAL;
```
<!-- @formatter:on -->

## Curator setup

TwTkms is relying on [tw-leader-selector](https://github.com/transferwise/tw-leader-selector), which in turn needs a specific
connection listener to be registered, before the `CuratorFramework` is started.

If you have your own configuration class for creating `CuratorFramework` bean, you can just remove it and it will be replaced with a good one.

`tw-leader-selector` is bringing in [tw-curator](https://github.com/transferwise/tw-curator) which does the correct auto configuration by itself.

Just set the following `tw-curator.zookeeper-connect-string` configuration option, and you are done.

For example:

```yaml
tw-curator:
  zookeeper-connect-string: "localhost:2181"
```

If you are using GradleWise, the Zookeeper path is automatically set using environment variables. You can use the following configuration:

```yaml
tw-curator:
  zookeeper-connect-string: ${ZK_SERVICE_HOST:localhost}:${ZOOKEEPER_PORT_MAPPING:2181}
```

## Multiple datasources

Some services have multiple data sources and TwTkms needs to know which one to use.

For that, you can annotate the correct one with `@Tkms` annotation.

Alternatively, for more complex setups you can provide an `ITkmsDataSourceProvider` implementation bean.

## Choosing a compression algorithm

A typical transfer change event compressed 100000 times:

```
Original size: 3237
Snappy time: 16057ms.
Snappy size: 1478
Gzip time: 36970ms.
Gzip size: 1044
LZ4 fast time: 7881ms.
LZ4 fast size: 1484
```

LZ4 is recommended as a default.

However, when you are using a cloud database with expensive storage, Gzip is recommended instead.
