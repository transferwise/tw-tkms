# Failure scenarios and troubleshooting

Not that those are likely, but they can give you more insight how things work internally.

### Topic does not exist

When you are registering a message, TwTkms will check if a topic exists and is available for writing.
 
However, if that topic is deleted afterwards, before the Proxy can send a message out, it will block the processing of that shard-partition entirely.

So make sure, you are not suddenly deleting topics :). 

If that happens, you can ask Messaging team to put that topic back or you need to ask DBAs to delete a specific row from a database (the storage id will be in error message). 

### Corrupted message in a database

Any corrupted message in a shard-partition table can halt processing of that shard-partition.

>> Probably could happen only when a new TwTkms version is released without full backward-compatibility.
>> To mitigate that risk, make sure you upgrade the library at least once per quarter.

If you don't have a DBA in hand, you can temporarily configure the application to send new messages to another, healthy shard.

Or you can implement an `ITkmsMessageInterceptor` which will save the message to another table (DLQ) and return `Result.DISCARD`