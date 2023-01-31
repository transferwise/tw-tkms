# Performance considerations

## Throughput
The total throughput will depend on numerous factors
* message content size
* database performance and latency, how much its resources are used for rest of the service components
* number of shards and partition used
* average number of messages per transaction
* network throughput
* and others...

The only meaningful recommendation here is to monitor and tune. 

On a local laptop, where Kafka, MariaDb, service nodes and performance tests are running, **50,000 messages/s** has been achieved.
 - with message size of circa 1024 bytes
 - with 100 messages in one transaction
 - with 20 threads constantly sending the messages
 - 250 million messages sent through without throughput degradation
 
In a production where the ancillary processes are running in their own servers, the number can be considerably higher.

> Future performance tests running against real, separate Kafka and RDS database could give more insights.
> But for now it's already clear that it is much, much more efficient than for example tw-tasks. 

The main bottlenecks in local tests are around how many bytes we are pumping through different things, i.e. message size is the main factor.
That is why we do compress messages with ZLIB by default - application CPU is not an issue.

However, you need to consider, that you are not using your database only for TwTkms, so you still have to be careful about how many messages
you are registering in a time period. For example if you will try to insert 1000000 messages/s in 100 threads, you will probably cause
an outage.

You need to think through if you need to have additional concurrency controls, like rate limiters or bulkheads in place, which you can
tune according to your monitoring results.
 
> For example Payout Service concurrency is already controlled by its own concurrency policies, but some other services may not have 
> a similar systems in place and may need a simple rate limiter around the sender.

To increase the throughput, you can configure more partitions (and create additional tables), but this is usually not needed in a typical
Wise service. 

Consider batching of messages. The most beneficial is to make sure, that there is a single transaction around multiple messages sending.
As the library is used for Transactional Outbox Pattern, we can assume, this is mostly the case.

There is also an interface to send multiple messages at once, which will rely on JDBC batch-insert. It can give benefits, if you want
to go crazy on latency, but it may make your logic more complex and less readable.

> As usual with databases, batching is good, but do not create too large transactions. They can start affecting other aspects of your database,
> for example replication lag, long-lasting locks, or affecting the cleaning of various garbage.

#### Latency considerations
It is quite hard to give some guarantees for latency and it will fluctuate a lot.

> You can have a GC pause or Kubernetes thread scheduling pause. You can have some kind of hiccup in database or in Kafka and so on.

Current production statistics show something like 30ms average latency from registering to getting acknowledgment from Kafka.

But the real world numbers are still something we can get after more adoption of the library for various use cases.

Usually, adding partitions will reduce the latency.

However, in most cases the correct solution is to create specific shards for low-volume topics with low latency concerns.

