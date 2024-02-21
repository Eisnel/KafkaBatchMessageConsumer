# KafkaBatchMessageConsumer

This class library aims to encapsulate the solutions for a few challenges I have encountered when designing a .NET Kafka Consumer that handles messages in batch. Some of these challenges might seem minor at first, but when combined they have a "force multiplier" effect that increases complexity. My goal is to encapsulte them all in a helper class, so that sub-classes which implement this can concentrate on business logic without needing to worry about the following nuances.

This code is not ready for prime-time. This is still a conceptual prototype. This was based on an idea that was swimming in my head, and I needed to get it out before I lost it. What follows is a very verbose explanation of what I was trying to accomplish, based on my experience implementing Kafka consumers.

First, why is processing a batch of messages important? Some types of processing involve fetching some data about a message's entity prior to processing it. If each message is handled one-by-one, then the fetch operation can only fetch a single entity per query**. But if we process a batch of messages, the processor code can pre-fetch a batch of entities with a single query. Not only is this likely more efficient on the data store, but it reduces the network round-trip time (because each individual call across the network incurs latency, especially in the cloud). Your code can keep that pre-fetched data in memory for a short time while it iterates through each message performing the necessary logic (if processing a message causes that pre-fetched data to change, or if data changes so fast that pre-fetched data can become instantly out-of-date, then maybe re-examine if this approach is wise).

The .NET Confluent.Kafka library's IConsumer.Consume() doesn't have a built-in way to retrieve a batch of messages, like the corresponding Java Consumer does. Consuming a batch doesn't seem like a difficult problem (in and of itself), because you simply call Consume() multiple times. But there are a number of things to take into account:

First, you want to be smart about the amount of time you allow Consume() to wait when consuming a message. If you haven't yet fetched any messages for your batch, then you can allow Consume() to wait for the first message to appear. But if you've already consumed at least one message, then you shouldn't let Consume() block for any amount of time, because it's [usually] better to process the messages you've already loaded rather than waiting for a full batch come along. This logic is handled by this library's class. But that's not the only problem. The challenges below add the problems incurrerd by processing a batch of messages at a time...

When processing a message, if you take too long the Kafka broker will assume that your consumer has ceased to exist, so it will revoke your assigned Partitions due to inactivity. For most workflows, processing a single message shouldn't take so long that this will happen, which I why I suspect that this is often ignored. But if a message does take a really long time, it's possible that by the time it finishes, the Kafka broker has given us up for dead and attempted to revoke our Partitions (and reassign them to some other consumer in our Consumer Group). The good news is that Kafka will wait a bit longer for us to confirm that revocation, so it's possible that this would allow us time to finish processing, even though immediately afterward we would still lose our Partitions. But if processing takes even longer and we don't confirm the Partition revocation in time, then the worse case scenario happens: Kafka gives our Partitions to a different consumer, which will then process the message that we're processing (since we never advanced that Partition's Offset***). So now we're double-processing a message.

Okay, but maybe you're saying: "My single message processing is unlikely to ever take that long, so this is a corner case." That's true, but corner cases still need to be handled. Furthermore, consider this: What if you're processing a batch of messages (for example, 100)? Processing the entire batch might naturally take longer than the allowed idle period. So the "heartbeat" must be kept alive while you're processing that batch. This library's KafkaPartitionAlternatingBatchConsumer class does that.

## KafkaPartitionAlternatingBatchConsumer

The "heartbeat" is handled whenever IConsumer's Consume() is called. That Consume() method does four things:
1. It gets the next message.
2. It lets Kafka know that this consumer is active (the "heartbeat").
3. It pre-fetches messages from the Kafka broker so that they're "on deck" in local memory, ready to be quickly returned by a future call to Consume().
4. It handles Partition revocation (as well as new Partitions being acquired).

When your sub-class is processing a batch of messages (in a different thread), the KafkaPartitionAlternatingBatchConsumer super-class is running an "event loop" that is constantly calling Consume(), but not actually consuming anything. This keeps the "heartbeat" going and allows the pre-fetching of data from the broker. If that call to Consume() returns a message, it is immediately put back into the IConsumer using Seek().

Any call to IConsumer's Consume() might trigger a Partition Revocation. The Kafka broker will rebalance the Partition assignments as other consumer instances appear (for the same "Consumer Group"), so this will happen with some regularity. If you had just been processing a single message at a time, then this wouldn't be a problem. Here's the very simple pattern:
1. You call IConsumer's Consume() to get the next message.
2. You process that message.
3. While you're processing, the broker decides that it needs to revoke two of the Partitions you've been assigned (for sake of argument, let's say that the message you're processing belongs to a Partition that's being revoked).
4. Good news: the broker won't immediately revoke those Partitions, it will wait (for a certain grace period) for you to confirm the revocation. So your message processing can continue (for a little bit longer).
5. You finish processing the message and Commit its offset.
6. At this point, the message's Partition has not yet been revoked, because the broker is waiting for you to confirm that you know about the impending revocation.
7. You call IConsumer's Consume() to get another message.
8. Within that call to Consume(), the Partition Revocation handler is invoked to inform you that you're about to lose a Partition.
9. In this simple scenario, you don't really care about this, so you do nothing in that handler. But as far as Kafka is concerned, you've now acknowledged this Partition revocation.
10. The Consume() call returns the next message from any Partitions that are *still* assigned to you (or no message if you have no assigned Partitions or if there are no available messages).
This is a simple pattern, because you only do something for steps #1, #2, and #7, everything else is handled by the IConsumer (though, as mentioned elsewhere, ignore Partition revocations at your peril). But this simple pattern only applies when messages are consumed, processed, and commited one at a time. But we want to handle messages in batch.

What happens if a Partition Revocation occurs while you're processing a *batch* of messages? Well, if you don't call Consume() at all while you're processing, then the revocation won't occur immediately, so you might be able to keep processing and Commiting all of your messages prior to revocation being finalized (which will happen during your next call to Consume()). But, refer back to the challenge above: What if batch processing potentially takes *longer* than the allowed timeout period? The Kafka broker is giving you a "grace period" to recognize the Partition revocation, but you might exceed that grace period. Also, it would be best if you gracefully stopped processing the batch of messages immediately when a revocation notification occurs, even if you weren't yet done with the whole batch. By "graceful", I mean that you can finish the currently in-flight message that you're processing, but you shouldn't proceed to the next message in your batch.

This library's KafkaPartitionAlternatingBatchConsumer class handles the above for you: As stated earlier, it keeps the "heartbeat" alive in the main "event loop" thread while your code processes a batch in a different thread. If the abstract class detects a Partition revocation --and if one of the messages in your batch belongs to that Partition-- then a CancellationToken will be cancelled alerting you that you should "gracefully" wrap-up processing (which might mean that you don't process every message in your batch).

Finally, this class makes it much easier to handle Committing Offsets when you're done processing messages. And it allows you to commit each message's Offset nearly immediately after processing, rather than waiting for the entire batch to finish.

When you implement the abstract message to process a batch of messages, two things are provided to you (in addition to the list of messages): An IAcknowledgement and a CancellationToken. The IAcknowledgement is a custom interface defined by this library. When you finish processing each message in a batch, simply use the IAcknowledgement to "Ack" that message. You can use it as many times as you want. An added bonus is that it won't make you wait for the Commit to be delivered to the Kafka broker, that will happen asynchronously, so the call to Ack returns instantanously. Your processor should not be slowed down waiting for interactions with the Kafka broker; those are done in a separate thread.

The passed CancellationToken will let you know if you should "gracefully" stop processing the current batch. This means that you can finish whatever message you have in-flight, and you should then Ack that message. But after that, you should stop if the CancellationToken has been triggered. As mentioned above, cancellation can occur if a Partition is revoked. It can also occur if this consumer has been ordered to shutdown. Let's say your long-running consumer is run inside of a Kubernetes Pod. When Kubernetes sends to the command to shut down, you can capture that and call this consumer's Shutdown() method. That will initiate a "graceful" shutdown, allowing the processor to finish any message that's in-flight, and ensuring that any Ack'd messages are Committed to the broker prior to the "event loop" ending.

After you're done processing a batch, any messages that you didn't "Ack" (using the IAcknowlement) will be automatically "Nacked". In Kafka, this simply means that the Offset for this Partition stored in your Consumer Group won't be advanced past any of those Nacked messages. But this abstract class also needs to tell the IConsumer to perform an in-memory Seek() of those Nacked messages so that they'll be re-delivered.

This consumer is meant to be run as a long-running process. Because .NET's Kafka library is not async, the "Run" method in this consumer is similarly not async. It will take run the main "event loop" on a single thread and not release that thread until after it's been ordered to Shutdown(). This will frequently block that thread when calling IConsumer's Consume(). This is how the Confluent.Kafka library is intended to be used. If you're running this from a simple Main() method in a worker that's run in a container or by an Azure App Service's WebJob (for instance), then you can simply use the main thread to run this consumer's "event loop", because that's your thread to do with as you please for the lifespan of your program. If you want to run multiple consumers from your Main() method, then you'll need to put each of them on their own thread. You can use the Task Factory to start a long-running thread for each consumer to run in, but be sure to specify the "long-running" option when starting those Tasks. That tells .NET to create a new thread instead of using one from the Thread Pool. This is important, because if you let this long-running synchronous consumer run on a Thread Pool thread, it will hog a thread that's meant to be pooled and shared.

On the other hand, when your sub-class' processing method is invoked to process a batch of messages, that is an "async" method. Your processing logic will be called on a Thread Pool thread (separate from the main "event loop" thread). It's expected that when your async processor logic calls side-effects it does so using async methods. Therefore, every time a side-effect occurs the thread can be reclaimed by the Thread Pool, and when your async side-effect is finished your code will continue on another Thread Pool thread.

## KafkaPartitionAlternatingSingleMessageConsumer:

This abstract class inherits from KafkaPartitionAlternatingBatchConsumer. It allows your sub-class to process messages individually, instead of in bulk, but still gives you the advantage of bulk processing: It lets you "peek" at each batch of messages so that you can perform batch pre-loading. You can then store those pre-loaded results in your own instance variables. It will subsequently call a method you implemented to process each message individually, and from that method you can reference your own pre-loaded data. The next time the "batch peek" method is called is when you dispose of the old pre-loaded data and then pre-load the next batch's data.

This is a very common pattern for handling batch message processing: Querying necessary data in bulk, but making updates one-by-one.

## Footnotes

** Granted, it might seem like caching is an alternative to pre-loading. And nothing about what I'm doing here prevents caching as a performance optimization. But if you load a message and need to fetch data about the entity it represents, then the cache helps you the next time you process that same entity. The first time it's presumably a cache "miss", but the next time it might be a cache "hit" (unless you're storing everything in your cache at all times, in which case your cache is effectively your data store, and you can still benefit from batch-fetching from something like Redis). But if there are millions/billions of such entities in your database, and re-occurrences of the same entity don't happen very often, then it might not be useful to try to cache them (your cache might evict them before they're ever used again, either due to the sheer number of entities you're caching, or because their TTL expired). Therefore, the benefit of pre-loading a batch of messages is not necessarily rendered moot by a cache.

*** Earlier, this made the assumption that you would wait to Commit a message's offset until after you're finished processing a message. The alternative is to Commit the message's offset prior to processing it. There are two competing concerns in message-driven models: "At most once delivery" and "at least once delivery". Obviously, you wish you could have both, right? And a lot of what I've done in this class is trying to get *close* to both. But, at the end of the day, you can only have one of those with absolute certainty. "At *most* once delivery" ensures that you won't ever double-process a message, but its downside is that a fault could result in the message not getting processed *at all*. "At *least* once delivery" ensures that a message will always get processed, because it doesn't get Committed until after processing is completed. But in the case of a serious fault which occurs between the end of processing and the Commit call, this message would be processed again (likely by a different consumer instance, since presumably the current consumer process is crashing and burning). This library chooses "At *least* once delivery", so Commits are intended to happen *after* processing. This library does a number of things to make this as safe as possible, to limit the risk of double-processing: It watches for Partition Revocation and gracefully alerts the processor to wrap-it-up mid-batch, which is intended to prevent the Kafka Consumer Group from giving up on us and forcibly reassigning our Partitions without our consent, which would otherwise result in double-processing.
