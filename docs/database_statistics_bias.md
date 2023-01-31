# Database statistics bias

`Tkms` is using tables, into which messages are inserted, then polled and afterwards deleted.

I.e. it has a rapid INSERT/DELETE type of workload.

This is a workload type, which all database are advising against using, where one reason among many is that quite awful query plans can easily be
derived.

Let's see how this usually happens.

Databases gather/calculate table and index statistics for the query planner to use, not in real time, but from time to time.

> Describing the mechanisms and triggers for statistics gathering, is out of the scope of this documentation. And, it is not necessary to know them.

Most of the time, `Tkms` tables are empty. So there is a very high chance that database statistics collector will register table and index statistics
at those points, registering that table is empty or at least almost empty.

When a database executes queries, it has to generate a plan for those. However, it uses the "old" statistics, which could easily have been taken hours
ago.

Now, let's assume that there is a spike of messages, where a substantial amount of records will be written into `Tkms` tables. Let's say we
accumulate 100k messages.

Now, let's look a simple query `Tkms` proxy component does: `DELETE from outgoing_message_0_0 where id in (1,2,3,4,5,6,7,8,9)`.

Now of course, any sane person would use PRIMARY key index here, but the database does not "know" the table has 100k records, its statistics are
telling it that the table is empty. If a table is empty or close to empty, the most efficient would be do it via sequential scan (aka full table scan)
and that is what database does, sequential scan over 100k records.

This of course can run very slowly. Worse, those queries will slow down the `Tkms` proxy cycle and the pace of sending messages to Kafka can
get behind the pace of registering new messages. Meaning, that table will continue growing, proxy cycle will get slower and slower and there
at one point those queries are monopolizing all database resources making everything for a service slow, not just `Tkms`.

And when you reach that situation, there is not much hope, that the database will recover from that situation by itself.

## Remedies

There are two main remedies for this from `Tkms` side: index hints and fixing statistics in place.

### Index hints

First, it tries to use force correct plans by applying index hints to all queries.
Index hints are built into MariaDb and MySql; and for Postgres there is the [pg_hint_plan](https://github.com/ossc-db/pg_hint_plan) extension.

Unfortunately MariaDb does not support index hints on `DELETE` queries. MySql does.

### Fixing statistics in place.

[setup guide](setup.md) will show you how.

In the case of MariaDb/MySql, it is very important to avoid running ANALYZE on those tables, because those will overwrite all our fixed statistics.

If this happens, it is important to fix the statistics back into place.

<!-- @formatter:off -->
```mariadb
update mysql.innodb_index_stats set stat_value=1000000 where table_name like "outgoing_message_%" and stat_description="id";
update mysql.innodb_table_stats set n_rows=1000000 where table_name like "outgoing_message_%";
flush table outgoing_message_*_*;
```
<!-- @formatter:on -->

## Manual intervention 

When `Tkms` tables have accumulated lots of records and sequential scans get executed, running `ANALYZE` on `tw-tkms` tables
will allow to overcome and recover the situation.

## Configuration change

The only query not covered by an index hint, is `DELETE` query for MariaDb.

In this case, the `delete-batch-sizes` property would allow to configure the queries to use less parameters per batch and thus having higher chance
of avoiding sequential scans.

E.g. 
```yaml
tw-tkms:
  delete-batch-sizes: "50, 10, 2, 1"
```
