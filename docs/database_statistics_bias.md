# Database statistics bias

`Tkms` is using tables, into which messages are inserted, then polled and afterwards deleted.

I.e. it has a rapid INSERT/DELETE type of workload.

This is a workload type, which all database are advising against using, where one reason among many is that quite awful query plans can easily be
derived.

Let's see how this usually happens.

Databases gather/calculate table and index statistics for the query planner to use, not in real time, but from time to time.

> Describing the mechanisms and triggers for statistics gathering, is out of the scope of this documentation. And, it is not necessary to know them.

Most of the time, `Tkms` tables are empty. So there is a very high chance that database statistics collector will register table and index statistics
at those times, registering that table is empty or at least almost empty.

When a database executes queries, it has to generate a query plan for those. For that, it uses the previously collected statistics.
Those statistic can easily be hours old. There is no magical mechanism for query planner is able to use some kind of "real time" data for itself.

Now, let's assume that there is a spike of messages, where a substantial amount of records will be written into `Tkms` tables. Let's say we
accumulate 100k messages.

Now, let's look a simple query `Tkms`'s `to-kafka-proxy` component does: `DELETE from outgoing_message_0_0 where id in (1,2,3,4,5,6,7,8,9)`.

Now of course, any sane person would use PRIMARY key index based query plan here, but the database does not "know" the table has 100k records.
Its statistics are telling it that the table is empty.
And in general, if a table is empty or close to empty, the most efficient query plan would be to execute sequential scan (aka full table scan)
and that is what database does - a sequential scan over 100k records.

This of course can run very slowly. Worse, those queries will slow down the `Tkms` proxy cycle and the pace of sending messages to Kafka can
get behind the pace of registering new messages. Meaning, that the table will continue growing, proxy cycle will get slower and slower and there
at one point those queries are monopolizing all database resources making everything for a service slow, not just `Tkms`. 
And the database could run into such a resource starvation that even the statistics collector would not be able to execute anymore.

And when you reach that situation, there is not much hope, that the database will recover from that situation by itself.

## Remedies

There are two main remedies for this in `Tkms` implementation:
* utilizing index hints
* fixing statistics into a place.

### Index hints

First, it tries to force correct query plans by applying index hints to all queries.
Index hints are built into MariaDb and MySql; and for Postgres there is the [pg_hint_plan](https://github.com/ossc-db/pg_hint_plan) extension.

Unfortunately MariaDb does not support index hints on `DELETE` queries. MySql does.

> So for MariaDb we still have a big risk - randomly ran `ANALYZE` statements.

> Even when we fix the statistic in place, telling database that there are large amount of records,
> a DBA accidentally running manual `ANALYZE`, at a time when the table is empty, will override those and thus negate the desired effect.

### Fixing statistics in place.

[setup guide](setup.md) will show you how.

In the case of MariaDb/MySql, as described above, it is very important to avoid running random ANALYZE on those tables, as
it will highly likely be run when the table is empty and thus overwriting all our fixed statistics to indicate tables with large amount of records.

If this happens, it is important to fix the statistics back into place.

<!-- @formatter:off -->
```mariadb
update mysql.innodb_index_stats set stat_value=1000000 where table_name like "outgoing_message_%" and stat_description="id";
update mysql.innodb_table_stats set n_rows=1000000 where table_name like "outgoing_message_%";
flush table outgoing_message_*_*;
```
<!-- @formatter:on -->

## Manual intervention

### Manual analyze

When `Tkms` tables have accumulated lots of records and sequential scans get executed, and we already have an incident, 
then running `ANALYZE` on `tw-tkms` tables will allow to overcome and recover from the situation. Even when we advised against
running `ANALYZE` above, then this time, the table has lots of records, so that `ANALYZE` will produce us suitable statistics.

### Configuration change

The only query not covered by an index hint, is `DELETE` query for MariaDb.

If accidental `ANALYZE` would override our custom in place index statistics, the bad queries can still happen.

In this case, the `delete-batch-sizes` property would allow to configure the queries to use less parameters per batch and thus having higher chance
of avoiding sequential scans.

> But it would be about reducing the probability and not eliminating the risk completely.

You can apply the custom configuration for deletion batch sizes as following. 

```yaml
tw-tkms:
  delete-batch-sizes: "50, 10, 2, 1"
```

### Phantom records hack.

`tw-tkms` polls records only with non-negative id. 

So, if we would create lots of records into those tables, with negative ids, and run `ANALYZE` afterwards, the database will
"remember" those tables having lots of records, and thus will be reluctant to run sequential scans against them.

> The downside here is that we may "waste" disk space, and it may look a bit messy.
> However messages on those records can be empty, so we would only keep 8 byte integers in those. 
> If we put 1 million records there, they may take "only" couple of tens of megabytes.
