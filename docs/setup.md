# Setup 
We are assuming you are using Spring Boot, at least version 2.1.

As usual, we can incorporate it into your Service through [tw-dependency](https://github.com/transferwise/tw-dependencies)
```groovy
implementation platform("com.transferwise.common:tw-dependencies:${twDependenciesVersion}")
implementation 'com.transferwise.kafka:tw-tkms-starter'
```

Configuration can be tweaked according to `com.transferwise.kafka.tkms.config.TkmsProperties`. Usually there is no need to change the defaults.

However, if you are using Postgres database, you need to change
```yaml
tw-tkms.database-dialect: POSTGRES
```

Of-course you need to create tables in the database as well.

For each shard & partition combination, you need a table in a form of `outgoing_message_<shard>_<partition>`.

>> If you are using Flyway, it can be achieved programmatically. For inspiration, check the `V1__Init` classes in this repo.

The tricky part is that those tables have to be created in exact way, to trick the database statistics not to consider
the table as empty and thus forcing it to rely more on primary key indexes. This is crucial to have minimum locking between the threads and
optimal performance even in cases where those tables will suddenly accumulate large number of messages.

## MariaDb

```mariadb
CREATE TABLE outgoing_message_0_0 (
              id BIGINT AUTO_INCREMENT PRIMARY KEY,
              message MEDIUMBLOB NOT NULL)
              stats_persistent=1, stats_auto_recalc=0 ENGINE=InnoDB;

update mysql.innodb_index_stats set stat_value=1000000 where table_name = "outgoing_message_0_0" and stat_description="id";
update mysql.innodb_table_stats set n_rows=1000000 where table_name like "outgoing_message_0_0";
flush table outgoing_message_0_0;
```

As some of those commands require specific permissions, you most likely will need some help from DBAs.

Also, it is beneficial (but not crucial) to set [innodb_autoinc_lock_mode](https://mariadb.com/docs/reference/es/system-variables/innodb_autoinc_lock_mode/) to 2.

## Postgres

```postgresql
CREATE TABLE outgoing_message_0_0 (
  id BIGSERIAL PRIMARY KEY,
  message BYTEA NOT NULL
) WITH (autovacuum_analyze_threshold=2000000000);

ALTER TABLE outgoing_message_0_0 ALTER COLUMN id SET (n_distinct=1000000);
VACUUM FULL outgoing_message_0_0;
```

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

## Multiple datasources

Some service have multiple datasources and TwTkms needs to know which one to use.

For that, you can use `@Tkms` annotation to mark a datasource to be used for tw-tkms.

Alternatively, in more complex setups you can provide an `ITkmsDataSourceProvider` implementation bean.
