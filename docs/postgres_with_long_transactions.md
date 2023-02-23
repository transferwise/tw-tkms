# Postgres with long-running transactions

## The problem

When `tw-tkms` is running on a Postgres database with long-running transactions, the following happens.

- Database starts using more and more CPU.
- `tw-tkms` throughput plummets.
- latency for sending out messages increases.

The longer the transactions take, the worse it gets.

The problem comes from the fact, that Postgres can not remove dead tuples, until the last transaction possibly needing those has finished.

So if we look at the messages polling with `select id, message from outgoing_message_0,0 order by id`, it starts to look
like following on database side, for new transactions.

```
message #0 (dead tuple)
message #1 (dead tuple)
...
message #1000000 (dead tuple)
message #1000001 (live tuple)
```

A person without understanding about Postgres internals, would assume the query above would be lightning fast, but it is not. It actually iterates
over all those million dead tuples and then returns 1 record.

It can be described with the following pseudocode:

```
record = findARecordFromPrimaryIndexVisibleForAnyTransaction()

// record value is #0

do{
    if (record.isVisibleToCurrentTransaction()){
        result.append(record)
    }
}
until record.nextInPrimaryIndex() == nil
```

The same situation can happen also when autovacuum is not well tuned and/or just too slow. Make sure your DBAs are paying attention on tuning that
properly.

## The solution

In the previous case, if we could also add a clause `where id >= 1000000`, the `findARecordFromPrimaryIndexVisibleForAnyTransaction`
can find the right record to start looping from, instantly, and the whole query will be again very fast.

What `tw-tkms` can do for you, is to track ids it has loaded from the database in a sliding window, let's say 5 minutes long by default.

And, then add `where id >= [earliest id in 5 minute period]` clause to every polling query.

To enable it, you need to create a tracking table and then turn it on from config.

```postgresql
CREATE TABLE tw_tkms_earliest_visible_messages
(
    shard      BIGINT NOT NULL,
    part       BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    PRIMARY KEY (shard, part)
) 
```

```yaml
tw-tkms:
  earliest-visible-messages:
    enabled: true
    look-back-period: 5m
```

## The risk

If you configure the `look-back-period` too small, you may have longer transactions adding messages too late and
`tw-tkms` will not see those anymore. I.e. you will end up messages in database which nothing is sending out.

You should be really sure about how long transactions, in which you are sending out tw-tkms messages, your application  
creates and protect yourself against having longer ones. 

Postgres lacks a way to directly set a maximum timeout for a transaction, but you can use `tw-reliable-jdbc`, which
at least helps to keep `statement_timeout` and `idle_in_transaction_session_timeout` small, making longer than expected
transactions highly improbable.

> It is highly recommended running this solution together with tw-reliable-jdbc integration.

If the worst case happens and some messages are left behind into the outbox tables, you can turn the
`tw-tkms.earliest-visible-message.enabled` to `false` and do a temporary deployment. 

> But notice in this case, ordering guarantees for those left-over messages are not met anymore.
