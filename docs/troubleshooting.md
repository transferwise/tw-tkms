# Failure scenarios and troubleshooting

Not that those are likely, but listing some hypothetical ones, can give you more insight how things work internally.

### Topic does not exist

When you are registering a message, TwTkms will check if a topic exists and is available for writing.

> Same as a direct kafka producer does. The check is fast and cached.
 
However, if that topic is deleted afterwards, before the Proxy can send a message out, it will block the processing of that shard-partition entirely.

So make sure, you are not suddenly deleting topics :). 

If that happens, you can ask Messaging team to put that topic back or you need to ask DBAs to delete a specific row from a database
(the storage id will be in error message). 

### Corrupted message in a database

Any corrupted message in a shard-partition table can halt processing of that shard-partition.

> Probably could happen only when a new TwTkms version is released without full backward-compatibility.
> To mitigate that risk, make sure you upgrade that library (and any other tw library :) ) at least once per quarter.

If you don't have a DBA in hand, or you have many random rows corrupted,
you can temporarily configure the application to send new messages to another, healthy shard. While you are working on a fix for the
broken shard.

There is a possibility to implement and register an `ITkmsMessageInterceptor` bean which will save the message to another table (DLQ) or 
just log it out and return `Result.DISCARD`.