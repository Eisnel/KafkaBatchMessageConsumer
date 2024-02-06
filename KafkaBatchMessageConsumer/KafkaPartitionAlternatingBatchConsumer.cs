using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace KafkaBatchMessageConsumer
{
    /// <summary>
    /// <para>
    /// Consumes batches of messages (ConsumeResults) from any number of Topics. Make a sub-class which implements
    /// ProcessBatchAsync(). That will receive a batch of Messages that it should process, in order. It will also recieve an
    /// IAcknowledgement object that it can use to mark each Message Ack'd (again, in order). When this returns, any un-Ack'd
    /// Messages will be re-delivered.
    /// </para><para>
    /// This offers a few advantages over calling IConsumer directly: First, it handles Consumption and Committing of
    /// messages in a background thread, so those things occur concurrently while you're processing a batch. While you're
    /// processing a batch it's consuming the next batch. Also, you can incrementally Ack messages and they'll be Committed
    /// in the background.
    /// </para><para>
    /// Since this loads a batch of ConsumeResults, the Partition Revokation/Lost Handler is especially important.
    /// This implements that Handler, which will check the current batch to see if any of its ConsumeResults belong to a
    /// Partition that's being revoked. If so, it will ask the async processor to gracefully stop processing (after it
    /// finishes anything that it's in the middle of), then it will commit whatever the processor finished before letting
    /// the revoked Partitions be released.
    /// </para><para>
    /// If ProcessBatchAsync() throws an exception, this whole consumer will be shutdown. If your processor suffers a
    /// transient fault and wishes to delay before having Messages re-delivered, it can simply delay itself. It runs in a
    /// different thread, so this consumer's main thread will continue to keep the IConsumer alive.
    /// </para><para>
    /// Another important feature: The IConsumer, by default, will get locked onto a single Partition as long as it has more
    /// messages. So even if your IConsumer has been assigned multiple Partitions (perhaps across multiple Topics), it won't
    /// move to the next Partition until the current one is finished. Imagine if you Subscribe to multiple Topics, and Topic
    /// A has a big backlog: a Partition with 50,000 messages, the oldest of which is a half hour old. Another Topic B you're
    /// Subscribed to has a Partition containing just a single message which is a few seconds old. When the IConsumer moves
    /// to Topic A's Partition, it won't move past it until it has Consumed everything in it (all 50K messages). While this
    /// is happening, Topic B's Partition's message will be waiting. Why is that a problem? Imagine that each Topic is a
    /// different "tenant". This creates a "noisy neighbor" problem: The client on Topic A has a long backlog, whereas the
    /// client on Topic B is quiet. Tenant A knows that its heavy volume will take a while to process. But Tenant B is
    /// expecting its single message to process quickly; a long wait doesn't make sense to that client. It would be really
    /// hard to tell Client B: Your single operation took a half hour because it had to wait until we completed processing a
    /// different client's records. Tenant B shouldn't be disadvantaged by a "noisy neighbor". When we're using common
    /// resources to process multiple tenants, we should be sure to interleave all of the Topics (and all of their
    /// Partitions). It's sort of like a "zipper merge" on the highway. A car entering an on-ramp shouldn't have to stop for
    /// a half hour to wait for rush hour traffic to completely finish before entering. Rather, a car on the highway should
    /// allow the on-ramp traffic to merge in an alternating pattern. Note that this example doesn't just apply to multiple
    /// Topics. It applies to Partitions within Topics as well. So even if there's only a single Topic being consumed, a
    /// Consumer might be assigned multipe Partitions. The same logic applies here: We shouldn't wait for one large Partition
    /// to completely finish before processing any messages from the other Partitions, we should instead interleave ("zipper
    /// merge") the Partitions.
    /// </para><para>
    /// This Consumer has a solution for that: After a certain number of messages have been Consumed from a Partition, this
    /// will force the Kafka IConsumer to move to the next Partition, even if there were more messages in the current
    /// Partition. It instructs the IConsumer to momentariliy Pause the Partition, just long enough for the IConsumer to
    /// increment to the next Partition in its circular queue, after which it immediately Resumes that Partition. This
    /// ensures that you're always cycling through Partitions, not getting stuck on ones with a deep backlog.
    /// </para>
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    ///
    public abstract partial class KafkaPartitionAlternatingBatchConsumer<TKey, TValue> : IKafkaPartitionAlternatingBatchConsumer
    {
        public string Name { get; }
        public int MaxMessagesPerBatch { get; protected set; }
        public int MaxConsecutiveMessagesPerPartition { get; protected set; }
        public int ShutdownGracePeriodInMilliseconds { get; set; } = 10 * 1000;
        public Exception? FatalException => _exceptionDispatchInfo?.SourceException;
        //protected bool SafeToReuseMessagesList { get; set; } = false;
        protected ConsumerBuilder<TKey, TValue> ConsumerBuilder { get; }
        public bool Running => _entranceGate == 1 && !Finished;
        public bool ShutdownOrdered => _shutdownOrderedSource.IsCancellationRequested;
        public bool Finished => _shutdownCompletedSource.IsCancellationRequested;
        public CancellationToken FinishedToken => _shutdownCompletedSource.Token;
        public IEnumerable<TopicPartitionWatermarkInfo> WatermarkStatistics => _topicPartitionWatermarkInfos.Values.ToArray();
        public TimeSpan Runtime => _runTimer.Elapsed;
        public TimeSpan NonIdleTime => _nonIdleTimer.Elapsed;
        public long AckedMessageCount => _totalCountOfAckedMessages;
        public double NonIdleThroughputPerSecond => NonIdleTime.GetInstancesPerSecond(_totalCountOfConsumedMessages);
        public double ProcessedMessageThroughputPerSecond => TimeSpan.FromTicks(_totalProcessingTimeInTicks).GetInstancesPerSecond(AckedMessageCount);
        public TimeSpan AverageMessageProcessingTime => TimeSpan.FromTicks(_totalProcessingTimeInTicks).GetAverage(AckedMessageCount);
        public TimeSpan AverageBatchProcessingTime => TimeSpan.FromTicks(_totalProcessingTimeInTicks).GetAverage(_processorTaskCount);
        protected bool StartThreadForEachBatch { get; set; }
        private readonly ConsumerConfig _consumerConfig;
        private IConsumer<TKey, TValue>? _consumer;
        private int _entranceGate = 0; // if zero then this has never been run, if 1 then someone entered the "gate" to start it running.
        private readonly CancellationTokenSource _shutdownOrderedSource = new();
        private readonly CancellationTokenSource _shutdownCompletedSource = new();
        private readonly Stopwatch _runTimer = new();
        private ExceptionDispatchInfo? _exceptionDispatchInfo;
        private readonly int _maxIntervalBetweenConsumesInMilliseconds;
        private readonly int _maxMillisecondsToWaitForAbortedTaskToFinish;
        private TopicPartition? _latestConsumedTopicPartition;
        private long _latestConsumedTopicPartitionConsecutiveCount = 0;
        private readonly TopicPartition?[] _pausedTopicPartition_inSingleElementArray = new TopicPartition?[1]; // an array with one element.
        private List<ConsumeResult<TKey, TValue>>? _activeBatch; // we'll reuse this List to reduce memory allocations
        private IReadOnlyList<ConsumeResult<TKey, TValue>>? _activeBatchReadOnly; // we'll reuse this to reduce memory allocations
        //private readonly List<ConsumeResult<TKey, TValue>> _bufferedBatch = new();
        private Acknowledgement? _acknowledgement; // we'll reuse this to reduce memory allocations
        private Task? _activeTask;
        private Stopwatch? _activeTaskTimer;
        private CancellationTokenSource? _abortProcessingCancellationSource; // used to ask the processor to stop (in a graceful manner)
        private CancellationToken? _activeTaskCompletedToken;

        private TimeSpan _consumeTimeWhenMessageRetrieved_excludingFirst;
        private long _countOfConsumptionBatchesWithAtLeastOneMessage;
        //private TimeSpan _consumeTimeNoWaitWhenMessageRetrieved;
        //private long _countOfConsumptionBatchesWithAtLeastOneMessageAndNoWait;
        private TimeSpan _consumeTimeWhenNoResults;
        private long _countOfConsumptionBatchesWithNoResults;
        private long _totalCountOfConsumedMessages; // how many messages have been Consumed (includes re-deliveries of Nack'd messages).
        //private long _totalCountOfMessagesSentToProcessor; // how many messages have been sent to the processor (includes re-deliveries).
        private long _totalCountOfAckedMessages; // how many messages have been Ack'd (tallied after they're Committed).
        private long _totalCountOfNackedMessages;
        private readonly Stopwatch _commitTimer = new();
        private long _commitOffsetCount; // count of Commit Offsets we've stored (we can Ack numerous messages with a single Offset Commit).
        private readonly Stopwatch _seekTimer = new();
        private long _seekCount; // count of Seek() operations.
        private readonly Stopwatch _pauseCallTimer = new();
        private long _pauseCallCount;
        private readonly Stopwatch _resumeCallTimer = new();
        private long _resumeCallCount;
        private readonly Stopwatch _nonIdleTimer = new();
        private long _totalProcessingTimeInTicks;
        private long _processorTaskCount;
        private int _highestProcessedBatchCount;
        private long _numberOfMaxedBatches;
        private readonly Stopwatch _delayTimer = new();
        private long _delayCount;
        private readonly Stopwatch _timeSinceLastCallToConsumer = Stopwatch.StartNew();

        private readonly Stopwatch _timeSinceLastWatermarkCollection = Stopwatch.StartNew();
        private const int cWatermarkStatCollectionIntervalInSeconds = 60;
        private readonly ConcurrentDictionary<TopicPartition, TopicPartitionWatermarkInfo> _topicPartitionWatermarkInfos = new();
        private const int cMaxWatermarkStatRetentionTimeInMinutes = 10;

        private static readonly object cStaticDebugMonitor = new();

        /// <summary>
        /// <para>
        /// </para>
        /// <para>
        /// Note: To set the IDeserializer, override the ConfigureConsumerBuilder() method.</para>
        /// </summary>
        /// <param name="name"></param>
        /// <param name="config">Note: If you're passing multiple Topics to this, then I highly recommend
        /// setting the PartitionAssignmentStrategy to either RoundRobin or CooperativeSticky
        /// (though CooperativeSticky might not yet be supported by this).
        /// Those spread out the work among multiple Topics & Partitions much better. If you
        /// use PartitionAssignmentStrategy.Range then each Consumer will be assigned the same
        /// Partition numbers across all of the Topics, which can result in a lot of starved Consumers.</param>
        /// <param name="maxMessagesPerBatch"></param>
        /// <param name="valueDeserializer">Your own IDeserializer that will deserialize the message's bytes to TValue.
        /// Passing this means that you have completely control of the deserialization.</param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="ArgumentException"></exception>
        public KafkaPartitionAlternatingBatchConsumer(string name, ConsumerConfig config, int maxMessagesPerBatch)
        {
            Name = name;
            if (string.IsNullOrEmpty(name))
            {
                string objectId = RuntimeHelpers.GetHashCode(this).ToString();
                Name = objectId[Math.Max(objectId.Length - 3, 0)..];
            }

            _consumerConfig = config ?? throw new ArgumentNullException(nameof(config));

            if (maxMessagesPerBatch <= 0)
            {
                throw new ArgumentException($"Invalid {nameof(maxMessagesPerBatch)}: {maxMessagesPerBatch}, cannot be less than 1.", nameof(maxMessagesPerBatch));
            }
            MaxMessagesPerBatch = maxMessagesPerBatch;
            MaxConsecutiveMessagesPerPartition = Math.Max((int)(MaxMessagesPerBatch * 2.5), 30);

            // We need to set the following configs in order to properly use the StoreOffset() feature: (Note:
            // EnableAutoCommit corresponds to "enable.auto.commit" and EnableAutoOffsetStore corresponds to
            // "enable.auto.offset.store"). Normally, when EnableAutoCommit is true that immediatley Commits every message
            // that's Consumed, which is not what we want because we explicitly Commit messages by asking the processor to
            // "Ack" them. But here's the fun part: The Auto-Commit feature simply saves whatever is in IConsumer's in-memory
            // "Offset Store". If EnableAutoOffsetStore is true then whenever a message is Consumed the in-memory Offset
            // Store is increased. By setting EnableAutoOffsetStore to false, we tell the IConsumer to NOT increase its
            // Offset Store on Consumption, but instead we have to manually increase its in-memory Offset Store by calling
            // IConsumer.StoreOffset(), which we do after our processor "Acks" a message. Once we manually call
            // StoreOffset(), the IConsumer's enabled Auto-Commit feature will occassionally commit that Offset to the Kafka
            // Consumer Group in the background. So that's why we set these two configs like this. We set EnableAutoCommit to
            // true, because if it's false then nothing will get committed to the Consumer Group, so messages will be
            // re-delivered. We set EnableAutoOffsetStore to false, because if it's true then messages will be
            // Committed/Ack'd the moment they're read, prior to our processor seeing them, which can result in undelivered
            // messages.
            config.EnableAutoCommit = true;
            config.EnableAutoOffsetStore = false;

            config.FetchMinBytes ??= 200;
            config.FetchMaxBytes ??= 52428800;
            config.FetchWaitMaxMs ??= 250;

            // The longest we want to wait between calls to IConsumer.Consume():
            _maxIntervalBetweenConsumesInMilliseconds =
                (int)(0.75 * (Min(config.HeartbeatIntervalMs ?? 3000, config.SessionTimeoutMs ?? 45000, config.MaxPollIntervalMs ?? 300000) ?? 4000));
            // I don't feel comfortable having this value be over 10 seconds:
            _maxIntervalBetweenConsumesInMilliseconds = Math.Min(_maxIntervalBetweenConsumesInMilliseconds, 10 * 1000);

            // After "session.timeout.ms" elapses without polling our IConsumer's Consume(), the Kafka Consumer Group
            // concludes that our IConsumer is dead and re-assigns all of its TopicPartitions. So if we need to wait for a
            // cancelled Task to finish, ensure that we wait less than "session.timeout.ms":
            _maxMillisecondsToWaitForAbortedTaskToFinish = (int)((config.SessionTimeoutMs ?? 45000) * 0.7);

            // Set handlers to deal with Partition Revocation and Partition Lost:
            if (config.PartitionAssignmentStrategy == PartitionAssignmentStrategy.CooperativeSticky)
            {
                // TODO: To support 'CooperativeSticky' we need to use a SetPartitionsRevokedHandler
                // that doesn't return a result. For now we don't support that strategy.
                // But since I recently discovered that the return result of the
                // Partition Revoke Handler doesn't matter, then perhaps we can support this???
                throw new ArgumentException($"{nameof(KafkaPartitionAlternatingBatchConsumer<TKey, TValue>)}.ctor: " +
                    $"ConsumerConfig PartitionAssignmentStrategy 'CooperativeSticky' is not supported.");
            }

            ConsumerBuilder = new(config);

            // Just for informational purposes, set a callback whenever we are assigned TopicPartitions:
            ConsumerBuilder.SetPartitionsAssignedHandler(PartitionsAssignedHandler);

            // Handle Partitions that the Consumer Group is about to revoke.
            // This gives us the opportunity to "finish up", which might include
            // waiting for processing to finish and committing some Offsets.
            // We're not allowed to call the IConsumer's Commit() directly here,
            // but we can return a list of TopicPartitionOffsets which contain
            // incremented Offsets where necessary (if we don't return anything
            // for certain revoked TopicPartitions that simply means that we're
            // not making any last-minute changes to its Offset).
            // Note: the revocation doesn't occur until after this Handler returns.
            // So this handler can wait synchronously for processing to finish,
            // though it musn't wait too long because the Consumer Group would
            // timeout our Consumer and we'd lose everything.
            ConsumerBuilder.SetPartitionsRevokedHandler(PartitionsRevokedHandler);

            // Handle Partitions that we have "lost". This is differen than "revocation".
            // We're being notified that these Partitions have already been lost to us.
            // They have likely already been assigned to another Consumer that is actively
            // updating offsets. Therefore, we can't touch these Partitions anymore.
            ConsumerBuilder.SetPartitionsLostHandler(PartitionsLostHandler);

            ConsumerBuilder.SetErrorHandler(ErrorHandler);
        }

        /// <summary>
        /// <para>
        /// This is where the actual work is done. A sub-class implements this to "process"
        /// the next batch of Messages.
        /// </para><para>
        /// Please try to gracefully honor the passed CancellationToken. If you're in the middle of
        /// a process that you can't partially abort, then you can finish that and get it to a commit
        /// point even if the CancellationToken is triggered during that process. At that point you'd
        /// still use the IAcknowledgement to Ack the Message that you processed. But try to end as
        /// quickly as possible.
        /// </para><para>
        /// It should use the passed IAcknowledgement to "Ack" the Messages after it has processed them.
        /// Messages must be Ack'd in order (this doesn't mean that you have to process them in order,
        /// or that you can't process them in parallel, but you need to Ack in order). You'll notice that
        /// you don't pass Message objects to IAcknowledgement, instead you pass their [zero-based] index
        /// in the passed IReadOnlyList. It's important to understand that there is no such thing as "Nack".
        /// In Kafka there's a single Offset (per Partition) that keeps track of which Message should be
        /// delivered next. If you Ack a Message, then Kafka's Offset will be moved past that Message,
        /// and it won't be delivered again. Any Messages that you don't Ack will be re-delivered in the future.
        /// Therefore, the only way to move past a Message is by Ack'ing it. If a Message cannot be processed,
        /// then the sub-class' logic must decide how to handle it. If you don't want failed Messages to be
        /// re-delivered then you must Ack'd them. The only reason not to Ack a Message is if you want it to
        /// be re-delivered in the future. This is desirable if the failure is "transient" in nature; if you
        /// think that trying it again in the future might work. If you decide that you want a Message to be
        /// re-delivered (therefore you don't Ack it), this will also re-deliver all Messages after that one.
        /// So you shouldn't process any Messages after the one that you want to be re-delivered, because
        /// those Messages might be processed again. This makes it tricky to parallelize Messages within this
        /// function. You might not need to parallelize, because there can be multiple Consumer processes running.
        /// </para>
        /// </summary>
        /// <param name="messages"></param>
        /// <param name="acknowledgement"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        protected abstract Task ProcessBatchAsync(IReadOnlyList<ConsumeResult<TKey, TValue>> messages,
            IAcknowledgement acknowledgement, CancellationToken cancelToken);

        protected abstract void ConfigureConsumerBuilder(ConsumerBuilder<TKey, TValue> builder);

        public CancellationToken Shutdown()
        {
            return Shutdown(exceptionThatCausedShutdown: null);
        }

        protected CancellationToken Shutdown(ExceptionDispatchInfo? exceptionThatCausedShutdown)
        {
            if (_entranceGate == 0)
            {
                throw new InvalidOperationException($"{nameof(KafkaPartitionAlternatingBatchConsumer<TKey, TValue>)}.{nameof(Shutdown)}: Consumer '{Name}': " +
                    $"Cannot shutdown this instance because it has not yet been started. You must call {nameof(RunSynchronously)} first.");
            }

            if (exceptionThatCausedShutdown != null && _exceptionDispatchInfo == null)
                _exceptionDispatchInfo = exceptionThatCausedShutdown;

            // It's okay to call this multiple times. And it's okay to call this after shutdown is completed.
            _shutdownOrderedSource.Cancel();
            return FinishedToken;
        }

        public Task RunAsynchronouslyInLongRunningThread(IEnumerable<string> topics, CancellationToken externalCancelToken)
        {
            return Task.Factory.StartNew(
                () => RunSynchronously(topics, externalCancelToken),
                TaskCreationOptions.LongRunning);
        }

        /// <summary>
        /// <para>
        /// This is the main entry point to run this consumer's event loop. This runs synchronously,
        /// so it doesn't return until this consumer is completely shut down. The idea is that a
        /// thread is devoted to a single consumer. If you want to run multiple differnt consumer
        /// instances, you should create multiple long-running threads (preferably not from the
        /// Thread Pool), one for each. This method can only be called once per instance, and it
        /// can't be called again even if you wait for the method to finish.
        /// </para>
        /// <para>
        /// A sub-class can override this in order to handle setup and
        /// teardown tasks, but it will need to call this base method. And also be aware that this
        /// can throw an exception if the consumer was stopped due to an exception, so be sure to
        /// perform teardown in a catch block (or use ExceptionDispatchInfo to capture an exception
        /// and then re-throw it when you're done with teardown). Alternatively, you can handle
        /// teardown by adding a callack to the ShutdownFinished CancellationToken.
        /// </para>
        /// </summary>
        /// <param name="topics"></param>
        /// <param name="queueMessageAction">This Action will be called with a QueuedMessage
        /// for each message that's read by the Consumer for the passed Topics.
        /// Important: This method needs to return FAST! It should NOT process the message synchronously.
        /// It might add this message to a queue that feeds existing processor threads,
        /// or it might start processing in an asynchronous Task and return immediately.
        /// If this doesn't return immediately then the Consumer managed by this RunSynchronously()
        /// method might "lapse" and be disabled by Kafka.</param>
        /// <param name="externalCancelToken"></param>
        /// <exception cref="InvalidOperationException">Another thread has already called RunSynchronously
        /// on this instance.</exception>

        public virtual void RunSynchronously(IEnumerable<string> topics, CancellationToken externalCancelToken)
        {
            // This "gate" prevents more than one thread from calling RunSynchronously() (on the same instance). Try to
            // change it from 0 to 1. If it's already 1, then do not change it. This returns _entranceGate's original value,
            // if it's not zero then another thread already ran this.
            if (Interlocked.CompareExchange(ref _entranceGate, 1, 0) != 0)
            {
                // Then another thread already set entranceGate to a number other than zero.
                // Therefore, this thread is not allowed to run this.

                InvalidOperationException gateException = new($"{nameof(KafkaPartitionAlternatingBatchConsumer<TKey, TValue>)}" +
                    $".{nameof(RunSynchronously)}: Another thread already started this instance's Run() method. " +
                    (this.Running ? "It is still running. " : "It has shut down. ") +
                    $"For any given instance, Run() cannot be called more than once.");

                // Because a sub-class might be relying on the ShutdownOrdered and ShutdownFinished
                // to clean-up some resources that it already created, we need those to both be
                // cancelled:
                Shutdown(exceptionThatCausedShutdown: ExceptionDispatchInfo.Capture(gateException));
                _shutdownCompletedSource.Cancel();

                throw gateException;
            }

            try
            {
                _runTimer.Restart();
                _RunSynchronously(topics, externalCancelToken);
            }
            catch (OperationCanceledException)
            {
                // Do nothing with this.
            }
            catch (Exception ex)
            {
                // I don't expect an exception to make it here, but it might happen if there's a bug.
                if (!ex.IsOperationCancelled()) // ignore OperationCancelledException, even if it's inside an AggregateException.
                {
                    // Calling Shutdown(Exception) is an easy way to store this Exception. It will be thrown below.
                    Shutdown(ExceptionDispatchInfo.Capture(ex));
                }
            }
            finally
            {
                // Just in case Shutdown() was never called (this shouldn't happen).
                Shutdown();

                _runTimer.Stop();
                // Indicate (to anyone who cares) that this has finished:
                _shutdownCompletedSource.Cancel();
            }

            // If an exception was thrown and captured, throw it now:
            if (_exceptionDispatchInfo != null)
            {
                _exceptionDispatchInfo.Throw();
            }
        }

        private void _RunSynchronously(IEnumerable<string> topics, CancellationToken externalCancelToken)
        {
            _ = topics ?? throw new ArgumentNullException(nameof(topics));

            if (externalCancelToken.IsCancellationRequested || _shutdownOrderedSource.IsCancellationRequested)
                return; // last chance to abort before the IConsumer is created.

            // Allow the sub-class to modify this ConsumerBuilder. Note that the ConsumerBulider
            // will not allow the handlers that we set in the constructor to be replaced.
            // We can't call this from our constructor because sub-class constructors won't yet
            // have been run, which means that the sub-class implementation of this method won't
            // have access to instance variables set by those sub-class constructors.
            ConfigureConsumerBuilder(ConsumerBuilder);

            _consumer = ConsumerBuilder.Build();
            using (_consumer)
            {
                _RunSynchronouslyWithConsumer(_consumer, topics, externalCancelToken);
            }
        }

        private void _RunSynchronouslyWithConsumer(IConsumer<TKey, TValue> consumer, IEnumerable<string> topics, CancellationToken externalCancelToken)
        {
            _ = consumer ?? throw new ArgumentNullException(nameof(consumer));
            _ = topics ?? throw new ArgumentNullException(nameof(topics));

            Stopwatch reusableTimer = new();

            var comboShutdownToken = CancellationTokenSource.CreateLinkedTokenSource(externalCancelToken, _shutdownOrderedSource.Token).Token;

            try
            {
                reusableTimer.Restart();
                consumer.Subscribe(topics);
                reusableTimer.Stop();
                _timeSinceLastCallToConsumer.Restart();
#if DEBUG
                Debug.WriteLine($"Consumer '{Name}': Subscribed in {reusableTimer.Elapsed.ToStringPretty()}.");
#endif
                long loopCount = 0;
                long streakOfConsecutiveMessagesConsumed = 0;
                long streakOfNoMessagesConsumed = 0;
                Stopwatch consumeTimer = new Stopwatch();

                while (!comboShutdownToken.IsCancellationRequested)
                {
                    loopCount++;
#if DEBUG
                    if (_countOfConsumptionBatchesWithAtLeastOneMessage <= 2
                        || _countOfConsumptionBatchesWithAtLeastOneMessage % 50 == 0
                        || _countOfConsumptionBatchesWithAtLeastOneMessage >= (6 * 200) - 2
                        || streakOfNoMessagesConsumed % 10 == 2)
                    {
                        lock (cStaticDebugMonitor)
                        {
                            Debug.WriteLine("\n----------------------------------");
                            Debug.WriteLine($"{Name} (tid {Environment.CurrentManagedThreadId}):");
                            Debug.Write(GetStats());
                            Debug.WriteLine($"Streak of consecutive consumed messages: {streakOfConsecutiveMessagesConsumed}, " +
                                $"streak of no messages: {streakOfNoMessagesConsumed}");
                            if (_activeBatch?.Count == 0 && streakOfNoMessagesConsumed >= 3)
                                Debug.WriteLine($"%%% Probably done %%%");
                            Debug.WriteLine("----------------------------------");
                        }
                    }
#endif

                    // If there is an _activeTask and it has finished, then this will resolve it (by Committing Ack'd
                    // messages and Seeking back to Nack's messages). In that case, after this is done _activeTask will be
                    // null, which signals to us that we can start processing another batch of messages. If _activeTask is
                    // not yet finished, this will Commit any messages that are already Ack'd, but otherwise it won't do
                    // anything else. This allows us to incrementally Commit Ack'd messages while the _activeTask is running.
                    // Passing zero in the waitToFinishInMilliseconds argument tells this that it shouldn't spend any time
                    // waiting for _activeTask to finish. We want to do other stuff (below) while _activeTask is running. If
                    // _activeTask is null this won't do anything.
                    HandleAcknowledgementsAndTryToFinishActiveTask(consumer, waitToFinishInMilliseconds: 0);

                    if (_activeTask == null)
                    {
                        // If there's no _activeTask then try to consume the next batch from the IConsumer.
                        // Note: We've been calling the IConsumer continually, and we expect it to be internally
                        // pre-fetching messages. So its Consume() call should return quickly with the next batch.
                        // But if it has no pre-fetched messages then we'll allow it to wait for at least one new Message.

                        // Calculate the max amount of time that we should wait on the Consume() call:
                        const int timeoutMilliseconds = 60 * 1000;

                        // This will call IConsumer's Consume(). It will wait until finding at least one Message, or
                        // until the timeout is passed, or the externalCancelToken is cancelled. After waiting for one
                        // Message, if it can retrieve additional Messages with no additional wait time then it will do so
                        // (this is possible if IConsumer has pre-fetched and buffered multiple Messages). If
                        // externalCancelToken is cancelled then this will throw an OperationCanceledException which will
                        // break us out of this loop and cause us to shutdown (see code below). Note that we don't pass it a
                        // stopListeningToken because that's only useful if there's an _activeTask, which there is not. A
                        // note on performance: The IConsumer is constantly pre-fetching Messages. Even while an _activeTask
                        // is running, this thread will continually call Consume() simply to ensure that the IConsumer can
                        // send heartbeats and pre-fetch data. So by the time we get here, it is likely that IConsumer has a
                        // bunch of pre-fetched Messages sitting in-memory, ready to be returned to us instantly. If it
                        // doesn't then it's likely that there are no Messages availabe, in which case Consume() will wait up
                        // to timeoutMilliseconds before giving up (we'll keep looping and trying again, ad infinitum (or
                        // until externalCancelToken is cancelled).

                        _activeBatch ??= new(MaxMessagesPerBatch); // we'll reuse this instance on every subsequent iteration.
                        if (_activeBatch?.Count > 0)
                        {
                            // This is just a sanity check to ensure that we didn't make a logic error in our code.
                            // _activeBatch is reused, but it should have been cleared before we get here.
                            throw new Exception($"{nameof(KafkaPartitionAlternatingBatchConsumer<TKey, TValue>)}.{nameof(_RunSynchronously)}. " +
                                $"Consumer '{Name}': Logic error! The {nameof(_activeBatch)} List should have been cleared, " +
                                $"but it contains {_activeBatch.Count} messages.");
                        }

                        consumeTimer.Restart();
                        ConsumeMultiple(
                            consumer,
                            MaxMessagesPerBatch,
                            sinkList: _activeBatch, // we re-use this List. It's already been Cleared.
                            stopListeningToken: CancellationToken.None,
                            timeoutMilliseconds,
                            comboShutdownToken);
                        consumeTimer.Stop();
                        if (_activeBatch?.Count > 0)
                        {
                            // We consumed at least one Message (up to MaxMessagesPerBatch Messages) from the IConsumer.
                            // So start a new processor Task to handle those. A sub-class can implement ProcessBatchAsync and
                            // do whatever it likes to process these Messages. We provide that with an IAcknowledgement
                            // instance that allows the processor to tell us when Messages should be Ack'd (this can be done
                            // mid-flight, because our thread will continually monitor that IAcknowledgement while
                            // _activeTask runs).

                            // We're definitely not idle because we Consumed at least one message:
                            _nonIdleTimer.Start();

                            _acknowledgement ??= new(); // this is reusable, but instantiate it if it's null.
                            _acknowledgement.Init(_activeBatch.Count); // it must always be initialized before it can be used.

                            _abortProcessingCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(comboShutdownToken);

                            Stopwatch taskTimer = Stopwatch.StartNew();
                            _activeTaskTimer = taskTimer;

                            _activeBatchReadOnly ??= _activeBatch.AsReadOnly(); // this can be reused. It will always read-through to the reused _activeBatch.

                            ThrowIfAnyTopicPartitionsNotAssignedToThisConsumer(_activeBatch.Select(cr => cr.TopicPartition));

                            // Start the processor Task (in the Thread Pool).
                            // We do not away this Task! Our loop must continue in our thread while this Task runs.
                            if (StartThreadForEachBatch)
                            {
                                _activeTask = Task.Run(async () =>
                                {
                                    await ProcessBatchAsync(_activeBatchReadOnly, _acknowledgement, _abortProcessingCancellationSource.Token);
                                });
                            }
                            else
                            {
                                _activeTask = ProcessBatchAsync(_activeBatchReadOnly, _acknowledgement, _abortProcessingCancellationSource.Token);
                            }

                            // Make a CancellationTokenSource that will be triggered when the _activeTask is finished. We
                            // plan to pass this CancellationToken to various things, which will cause waits/delays to end
                            // the moment _activeTask finishes.
                            CancellationTokenSource thisTaskCompletedTokenSource = new();
                            _activeTaskCompletedToken = thisTaskCompletedTokenSource.Token;
                            //_activeTaskCompletedCancellationSource = thisTaskCancelTokenSource;

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                            _activeTask.ContinueWith(t =>
                            {
                                // Note: This occurs in the same thread that processed the batch, which means it's occurring
                                // on a different thread than our main loop. So be careful accessing these shared variables.

                                taskTimer.Stop(); // use local variable because instance variable may have been set to null.
                                Interlocked.Add(ref _totalProcessingTimeInTicks, taskTimer.Elapsed.Ticks);

                                Interlocked.Increment(ref _processorTaskCount);

                                // When this Task finished, cancel this CancellationToken.
                                thisTaskCompletedTokenSource.Cancel(); // use local variable because instance variable may have been set to null.
                            }, TaskContinuationOptions.ExecuteSynchronously);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

                            // Keep track of the last consumed TopicPartition, and how many consecutive messages we've
                            // consumed in that TopicPartition. We'll use this info to decide whether to Pause this
                            // TopicPartition in case we get locked-into it.
                            TopicPartition latestTopicPartitionInThisBatch = _activeBatch[_activeBatch.Count - 1].TopicPartition;
                            int consecutiveCount = _activeBatch.CountConsecutiveSamePartitionInReverse();
                            if (consecutiveCount < _activeBatch.Count || latestTopicPartitionInThisBatch != _latestConsumedTopicPartition)
                            {
                                // Then either this whole batch does not have the same TopicPartition, or the previously
                                // recorded TopicPartition doesn't match this batch's last TopicPartition. Either way, this
                                // means that we're starting a new streak with just the consecutive messages at the end of
                                // this batch, so overwrite these values:
                                _latestConsumedTopicPartition = latestTopicPartitionInThisBatch;
                                _latestConsumedTopicPartitionConsecutiveCount = consecutiveCount;
                            }
                            else
                            {
                                // Otherwise the last TopicPartition in this batch matches the previously remembered
                                // TopicPartition. Furthermore, this entire batch has the same TopicPartition. Therefore, the
                                // same uninterrupted streak continues, so add to its count:
                                _latestConsumedTopicPartitionConsecutiveCount += consecutiveCount;
                            }

                            // Track some statistics:

                            if (_activeBatch.Count > _highestProcessedBatchCount)
                                _highestProcessedBatchCount = _activeBatch.Count;
                            if (_activeBatch.Count >= MaxMessagesPerBatch)
                                _numberOfMaxedBatches++;

                            // Note that we only track these consumption time stats when there's no _activeTask, because we
                            // want to determine how long we wait when looking for the next batch of messages. There's
                            // another part of our code that calls Consume() just to keep the heartbeat alive (see below),
                            // but we don't track its consumption time because it's happening in parallel with an
                            // _activeTask, so it doesn't represent any sort of "latency" that we need to be concerned with.

                            if (_countOfConsumptionBatchesWithAtLeastOneMessage > 0)
                            {
                                _consumeTimeWhenMessageRetrieved_excludingFirst += consumeTimer.Elapsed;
                            }
                            _countOfConsumptionBatchesWithAtLeastOneMessage++;
                            _totalCountOfConsumedMessages += _activeBatch.Count;

                            streakOfNoMessagesConsumed = 0;
                            streakOfConsecutiveMessagesConsumed++;
                        }
                        else // no messages were consumed
                        {
                            // Track some stats about our failure to retrieve any Messages. Note that we only care about
                            // these statistics when there is no _activeTask, because this represents time spent "idle".

                            _consumeTimeWhenNoResults += consumeTimer.Elapsed;
                            _countOfConsumptionBatchesWithNoResults++;

                            streakOfConsecutiveMessagesConsumed = 0;
                            streakOfNoMessagesConsumed++;

                            // Since no messages were consumed, don't make any changes to the
                            // _latestConsumedTopicPartition and _latestConsumedTopicPartitionCount.
                        }

                        CollectStats(consumer, _activeBatch);
                    }

                    if (_activeTask != null)
                    {
                        // There is an _activeTask running in a seperate thread thread. In that case, this main
                        // thread loops and does some maintenance work. One very important job is to frequently call the
                        // IConsumer's Consume() method. This is necessary for these reasons: First, this allows the
                        // IConsumer to send a heartbeat to the Kafka Broker so that our IConsumer isn't considered to be
                        // AWOL and lose its Partitions (via rebalancing). Second, the IConsumer will pre-fetch Messages
                        // in the background. Although I'm not sure how these frequent calls to Consume() affect that
                        // (because I don't think it blocks Consume() while doing that), we might still need to frequently
                        // call Consume() in order to facilitate that. Third, a call to Consume() is necessary for
                        // Partition Assignment and Revocation to happen, because it is during that Consume() call that
                        // our Partition Assignment and Revocation Hanlders are called. Fourth, this allows us to
                        // temporarily Pause a TopicPartition that has had a long, uninterrupted streak, so that other
                        // assigned TopicPartitions can get a chance to be consumed. In order for this to work, Consume()
                        // must be called while the TopicPartition is Paused.

                        // Note that when we call Consume() in this capacity, we aren't trying to retrieve any Messages.
                        // Therefore, if we do retrieve a Message we'll put it back by using Seek(). That just tells
                        // the IConsumer to deliver it to us again (it's an in-memory operation).

                        // Avoid getting locked-into a TopicPartition which has a ton of messages. If there has been a
                        // long, uninterrupted streak of messages from a single TopicPartition then Pause that TopicPartition
                        // prior to the Consume() call, then Resume it immediately afterward. Note that we're doing this
                        // Pause/Consume/Resume step in this code that happens while waiting for a processor thread to
                        // complete. This is more efficient, because it doesn't do this in the code that actually pulls
                        // messages for processing (above).
                        PauseTopicPartitionThatIsOnLongStreak(consumer);

                        // TODO: If the _activeTask is completed and we didn't Pause a TopicPartition, then we can skip the
                        // rest of this and move to the next iteration of the loop. If a TopicPartition is Paused then we
                        // stay here because we need to deal with that Paused partition by calling Consume() (with no wait)
                        // and then Resuming that partition.

                        // But we need to be careful, because we don't want to enter into a tight loop: If we call
                        // Consume() and it immediately returns a Message (that we then put back), this would cause us to
                        // enter into a very tight loop where we do that repeatedly at high. speed Therefore, we want to wait
                        // a period of time. There are two different ways to wait: First, we can tell Consume() to wait a
                        // certain amount of time. This is the preferred way to wait, but if the IConsumer has any pre-fetched
                        // messages then it won't delay at all. Second, if Consume() doesn't wait long enough, then we can
                        // also use a synchronous delay. But how long should we wait? Well, we want to iterate back to the
                        // top of this loop a few times during the life of _activeTask so that we can see if any Messages
                        // have been Acknowledged and Commit them with the Kafka Broker. So we want to pick a time that's a
                        // fraction of the average time it takes for an _activeTask to process. That time is how long we'll
                        // ask Consume() to wait. Second, we want to ensure that we're calling Consume() once every
                        // _maxIntervalBetweenConsumesInMilliseconds, so that heartbeats can occur at regular intervals.
                        // We don't need to include that time period in the call to Consume(), because while Consume() is
                        // blocking it will handle heartbeats. But we do want to take that time into account if we do a
                        // synchronous pause/delay. So when we perform that latter pause/delay, we'll use the lesser of those
                        // two times.
                        int timeoutMilliseconds;
                        if (PausedTopicPartition != null)
                        {
                            // If we Paused a TopicPartition (above), then we'll use a zero timeout, here's why: We used
                            // Pause simply to make the IConsumer send the current TopicPartition to the end of its queue.
                            // Once we call Consume() we achieve that goal. But what if that Paused TopicPartition is the
                            // only partition that has any messages available? We don't want to wait for many seconds for
                            // messages to appear in the other TopicPartitions when we know for certain that there are
                            // messages in the Paused partition. So in that case we use a zero timeout. On the next iteration
                            // of this loop we'll Resume that paused TopicPartition, and then we can set a normal timeout for
                            // the Consume().
                            timeoutMilliseconds = 0;
                        }
                        else
                        {
                            // So we want to allow Consume() to wait some amount of time to avoid a tight loop. But there's
                            // an _activeTask, so we don't want to wait too long on Consume() because we need to
                            // occassionally check to see if the active processor has Ack'd any Messages that need to be
                            // Committed. Therefore, we'll wait a fraction of the average time it takes the processor to
                            // complete a batch:
                            timeoutMilliseconds = (int)AverageBatchProcessingTime.TotalMilliseconds;
                            if (_activeBatch?.Count > 19)
                                timeoutMilliseconds /= 4;
                            if (_activeBatch?.Count > 7)
                                timeoutMilliseconds /= 3;
                            else if (_activeBatch?.Count > 3)
                                timeoutMilliseconds /= 2;
                        }

                        // Do we need to make this "maintenance call" to IConsumer's Consume()?
                        // If we Paused a TopicPartition then we must call Consume() to let the IConsumer skip over that
                        // Partition (before we Resume() that Partition, below).
                        // Otherwise, we only need this "maintenance" call to Consume() if our max interval between Consume()
                        // calls has been exceeded. If we don't call Consume(), then the code below will simply perform a
                        // delay for a short amount of time.
                        bool maintenanceCallToConsumeNeeded = PausedTopicPartition != null;
                        if (!maintenanceCallToConsumeNeeded)
                        {
                            maintenanceCallToConsumeNeeded = _timeSinceLastCallToConsumer.Elapsed.TotalMilliseconds >= _maxIntervalBetweenConsumesInMilliseconds;
                        }

                        // Always time this call to Consume(), even if we don't call Consume(), because this is used in the
                        // logic below that determines how long we should delay.
                        consumeTimer.Restart();
                        if (maintenanceCallToConsumeNeeded)
                        {
                            // This will call IConsumer's Consume(). It will wait until the above timeout is passed, but it will
                            // end early if _activeTask finishes (we have a CancellationToken for that). This will also abort if
                            // the externalCancelToken is cancelled (but in that case it will throw an OperationCanceledException
                            // which will break us out of this loop and cause us to shutdown). We pass a maxMessageCount of zero
                            // to this because we don't want to get any Messages back. This method will wait on the Consume()
                            // method, and if a Message is returned it will "put it back" by calling IConsumer's Seek().
                            ConsumeMultiple(
                                consumer,
                                maxMessageCount: 0, // because maxMessageCount is zero, this method will always returns a null List.
                                sinkList: null,
                                stopListeningToken: _activeTaskCompletedToken ?? CancellationToken.None,
                                timeoutMilliseconds,
                                comboShutdownToken);
                        }
                        consumeTimer.Stop();

                        // Very important note: After calling ConsumeMultiple(), there MIGHT no longer be a running
                        // _activeTask, because the IConsumer's Consume() might call our Partition Revocation Handler, which
                        // can cancel the _activeTask and null the instance variables such as _activeTask.

                        // If we Paused a TopicPartition (prior to the Consume() call), then Resume it. The Pause only
                        // needs to be in place for a single call to Consume(). After that, the IConsumer will have moved to
                        // the next TopicPartition in its circular queue, so we can un-Pause the busy TopicPartition.
                        ResumePausedTopicPartition(consumer);

                        // If the Consume() call took less than timeoutMilliseconds --because there were available Messages--
                        // then we want to delay for the rest of that time (but there are some caveats, explained in comments
                        // below). But first, very important: If _activeTask was nulled by the above call to Consume() then
                        // we need to skip this. Note only because we want to immediately try to consume and retrieve the
                        // next batch of messages, but because instance variables such as "_acknowledgement" and
                        // "_activeTaskCompletedCancellationSource" will now be null. Also, if _activeTask is now completed
                        // then we don't wait, just proceed to the next iteration of the loop to handle that completed Task.
                        if (consumeTimer.Elapsed.TotalMilliseconds < timeoutMilliseconds && _activeTask != null && !_activeTask.IsCompleted)
                        {
                            // If the running _activeTask has Ack'd any Messages that we haven't yet Committed, then wait
                            // no longer. Also don't wait if any of any of our CancellationTokens have been cancelled
                            // (note: _activeTaskCompletedCancellationSource is a linked CancellationToken that includes
                            // the externalCancelToken, so we don't need to also check externalCancelToken).
                            bool hasUncommittedAckedMessages = _acknowledgement?.HasUncommittedAckedMessages ?? false;
                            if (!hasUncommittedAckedMessages && !(_activeTaskCompletedToken?.IsCancellationRequested ?? false) && !comboShutdownToken.IsCancellationRequested)
                            {
                                // Calculate the remaining time that we still need to delay (by subtracting how long we
                                // waited on the call to Consume(), above):
                                timeoutMilliseconds -= (int)consumeTimer.Elapsed.TotalMilliseconds;
                                // However, we also need to ensure that the time between calls to Consume() doesn't
                                // exceed _maxIntervalBetweenConsumesInMilliseconds. We may or may not have called
                                // Consume() above. So figure out how much time we have left before we must call
                                // Consume() again, and don't wait longer than that:
                                double timeUntilNextCallToConsume = _maxIntervalBetweenConsumesInMilliseconds - _timeSinceLastCallToConsumer.Elapsed.TotalMilliseconds;
                                // Use whichever is less:
                                timeoutMilliseconds = Math.Min(timeoutMilliseconds, (int)timeUntilNextCallToConsume);
                                if (timeoutMilliseconds > 0)
                                {
                                    // Delay synchronously, but resume the moment _activeTask is finished (or our
                                    // externalCancelToken is cancelled):
                                    DelaySynchronouslyDoNotThrowException(timeoutMilliseconds,
                                        _activeTaskCompletedToken ?? CancellationToken.None, comboShutdownToken);
                                }
                            }
                        }
                    }

                    // Note: See PartitionsRevokedHandler() for its very important contribution to this process. That is
                    // called when some TopicPartitions are being revoked/unassigned from us. It can perform some of the same
                    // actions as above, because it might need to prompt the active processor Task to gracefully shutdown and
                    // then Commit anything that the processor Ack'd. It can also make changes to our instance variables in
                    // order to remove all traces of the revoked TopicPartitions.
                }
            }
            catch (OperationCanceledException)
            {
                // Then the CancellationToken was triggered. In that case we don't throw an exception, we just end the loop.
            }
            catch (Exception ex)
            {
                // Then an unexpected/uncaught exception has caused us to stop. We eventually want to throw this exception,
                // but first we must Close() our IConsumer (below). Afterward, we'll throw this captured exception.
                if (!ex.IsOperationCancelled()) // ignore OperationCancelledException, even if it's inside an AggregateException.
                {
                    // Calling Shutdown(Exception) is an easy way to store this Exception..
                    Shutdown(ExceptionDispatchInfo.Capture(ex));
                }
            }

            // If we're here then the main loop (above) is finished. This can happen if we're ordered to shutdown,
            // or if a particularly nasty exception occurred (in that case, it's been captured by a call to
            // Shutdown(ExceptionDispatchInfo) and will be re-thrown by our caller).
            // So now we enter our "shutdown" phase.

            // In case we got here without our Shutdown() being called.
            Shutdown();

            // We MUST ALWAYS Close() our IConsumer when exiting this method. There may still be an active processor Task
            // (_activeTask) that was started during the "while" loop above; it's perfectly normal for that "while" loop
            // to end without resolving the _activeTask. Fortunately, IConsumer's Close() will handle all of that (read
            // on): It's imperative that we call Close(), because otherwise the Consumer Group will assume that we
            // maintain ownership of our assigned Topic/Partitions until the "session.timeout.ms" timeout expires (even
            // after our IConsumer is Disposed). Close() will explicitly release our Topic/Partitions back to the Consumer
            // Group so that they can be reassigned. But before that, Close() will cause our PartitionsRevokedHandler() to
            // be called. Invoking that handler allows our code to attempt to gracefully cancel the active processor Task
            // and then wait on it to finish. If it's in the middle of processing and beyond the point-of-no-return for
            // certain messages then it's allowed to finish processing those, and we can still Ack/Commit successful
            // messages from our PartitionsRevokedHandler().
            try
            {
                _timeSinceLastCallToConsumer.Stop();
                consumer.Close(); // our PartitionsRevokedHandler will run to completion before this returns. 
                _timeSinceLastCallToConsumer.Restart();
            }
            catch (Exception ex)
            {
                // I know that Shutdown() has already been called. This is just a convenient way to set the captured
                // exception. If there's already a captured exception, this won't overwrite it.
                Shutdown(ExceptionDispatchInfo.Capture(ex));
            }

            // Note: If an Exception was passed to Shutdown(ExceptionDispatchInfo) then the caller will throw that.
        }

        private bool HandleAcknowledgementsAndTryToFinishActiveTask(IConsumer<TKey, TValue> consumer, int waitToFinishInMilliseconds)
        {
            if (_activeTask == null)
                return false;

            // 1. Check to see if the _activeTask is non-null and completed, store that in a boolean variable.
            bool activeTaskWasCompletedPriorToCommitting = _activeTask?.IsCompleted ?? false;

            // 2. If there's a non-null _activeTask, see if any of its its messages have been Ack'd and Commit them.
            //    Note: The _activeTask doesn't need to be finished. We can Commit messages as they're Ack'd by the
            //    running processor.
            CommitAckedMessages(consumer);

            if (!activeTaskWasCompletedPriorToCommitting && waitToFinishInMilliseconds > 0)
            {
                int result = Task.WaitAny(new Task[] { _activeTask }, waitToFinishInMilliseconds);

                if (result != -1)
                {
                    // Then _activeTask completed while we waited for it, so Commit any additional messages that it Ack'd:
                    CommitAckedMessages(consumer);
                    // Now note that we Committed after the Task finished:
                    activeTaskWasCompletedPriorToCommitting = true;
                }
                else
                {
                    // Then _activeTask didn't finish. We don't need this "else", it's just here for debugging.
                }
            }

            if (!activeTaskWasCompletedPriorToCommitting)
            {
                // Then the _activeTask was NOT finished prior to Committing its Ack'd messages. So return saying that the
                // Task didn't finish. Note: It's possible that the Task finished while we were Committing, but in that case
                // there might be more messages that were Ack'd concurrently while we Committed, so we don't want to Nack
                // yet. Instead, we'll return false and let another iteration of the main loop handle this.
                return false;
            }

            // If we're here, then _activeTask is completed. Regardless of whether it finished successfully or Fauled,
            // we want to Nack any messages that weren't Ack'd.
            SeekBackToAllNackedMessages(consumer);

            // Since _activeTask is done and we've processed the outcomes of all messages in _activeBatch (either by
            // Committing the Ack'd messages or Seeking back to the Nack'd messages), we can now clear those instance
            // variables, which will allow the main loop to start a new processor and put its Task in _activeTask.
            // But keep a local reference to the Task because we need to handle its exception if it's Faulted.
            Task task = _activeTask;
            ResetActiveBatchAndTask();
            // Now that _activeTask is done we're "idle". This timer will be resumed the next time any messages are consumed.
            _nonIdleTimer.Stop();

            // Now that we've cleaned-up that finished processor Task, it's the moment of truth: Did that processor throw an
            // exception? If so, the following code will throw that exception (otherwise it will do nothing). Using
            // GetAwaiter() throws the exception without wrapping it in an AggregateException (though it's possible that the
            // internal exception was an AggregateException).
            task.GetAwaiter().GetResult();

            return true; // indicate that this handled the completed _activeTask.
        }

        protected void ThrowIfAnyTopicPartitionsNotAssignedToThisConsumer(IEnumerable<TopicPartition> topicPartitions,
            [System.Runtime.CompilerServices.CallerMemberName] string memberName = "")
        {
            foreach (var topicPartition in topicPartitions)
            {
                ThrowIfTopicPartitionNotAssignedToThisConsumer(topicPartition, memberName);
            }
        }

        protected void ThrowIfTopicPartitionNotAssignedToThisConsumer(TopicPartition topicPartition,
            [System.Runtime.CompilerServices.CallerMemberName] string memberName = "")
        {
            ThrowIfConsumerIsInvalid(memberName);

            if (_consumer != null && !_consumer.Assignment.Contains(topicPartition))
            {
                throw new Exception($"{memberName}: Consumer {Name}: TopicPartition {topicPartition} " +
                    $"is not assigned to IConsumer {_consumer.Name}, so processing cannot proceed. " +
                    $"Shutting down this consumer (tid {Environment.CurrentManagedThreadId}).");
            }
        }

        protected void ThrowIfConsumerIsInvalid([System.Runtime.CompilerServices.CallerMemberName] string memberName = "")
        {
            if (_consumer == null)
            {
                if (Running)
                {
                    throw new InvalidOperationException($"{memberName}: Consumer {Name} is running but IConsumer is null, " +
                        $"so processing should not be occurring. Shutting down this consumer (tid {Environment.CurrentManagedThreadId}).");
                }
                else
                {
                    throw new InvalidOperationException($"{memberName}: Consumer {Name} is not running, " +
                       $"so processing should not be occurring (tid {Environment.CurrentManagedThreadId}).");
                }
            }

            if (_consumer.Handle.IsInvalid)
            {
                throw new Exception($"{memberName}: Consumer {Name}: librdkafka handle is invalid for IConsumer {_consumer}. " +
                    $"Shutting down this consumer (tid {Environment.CurrentManagedThreadId}).");
            }
        }

        private TopicPartition? PausedTopicPartition => _pausedTopicPartition_inSingleElementArray[0];

        private void PauseTopicPartitionThatIsOnLongStreak(IConsumer<TKey, TValue> consumer)
        {
            // We want to avoid getting locked-into a TopicPartition which has a ton of messages. We might be assigned
            // multiple TopicPartitions, but the IConsumer will stick with one until it's empty, before moving to the next.
            // For example: If a TopicPartition has 10K messages, the IConsumer will [by default] burn through all of those
            // before moving to the next TopicPartition. This causes our consumption to be very unbalanced. Granted, we never
            // promised that messages would be processed in-order if they are in different TopicPartitions, but we at least
            // want to stay sort of close. Furthermore, we might be consuming from multiple Topics, and if each Topic is a
            // different Tenant then this causes a "noisy neighbor" problem, because Tenant B's TopicPartition won't be read
            // until Tenant A's TopicPartition is completely consumed. Here's how we fix that: If we've read too many
            // consecutive messages from a single TopicPartition then we Pause it in the IConsumer. The next call to
            // IConsumer.Consume() will then be forced to read from the next TopicPartition in its queue. After the next call
            // to Consume(), we will Resume that TopicPartition, because at that point IConsumer will have already moved to
            // the next TopicPartition in its queue. Note that this might be the only TopicPartition that has any Messages,
            // which is another reason why we want to resume this TopicPartition after only one call to Consume(). That's
            // also why we don't perform any waits (even waits while calling Consume()) when there's a Paused Partition.

            // TODO: Do we really need to wait until after a call to Consume() before Resuming the Paused TopicPartition?
            // When you call Pause(), does IConsumer immediately advance to the next TopicPartition in its circular queue, or
            // does it wait for a call to Consume() before doing that? It might be the latter because the "Pause" feature
            // might be communicated to the Kafka server. But maybe not. A test is in order, because it would be faster if we
            // can Resume the Partition immediately after Pausing it, rather than making a single call to Consume(). Why?
            // Imagine if the current Partition is the only one with any Messages. When we Pause it, the next call to
            // Consume() will retrieve nothing (because that Partition is Paused), then we Resume that Partition, after which
            // the next call to Consume() gets its next message. But that caused a wasted call to Consume().

            if (PausedTopicPartition != null)
            {
                // There's already a Paused TopicPartition. This is a logic error; it shouldn't ever happen.
                throw new InvalidOperationException($"{nameof(KafkaPartitionAlternatingBatchConsumer<TKey, TValue>)}" +
                    $".{nameof(PauseTopicPartitionThatIsOnLongStreak)}: This should never be called when a TopicPartition " +
                    $"is already paused. Yet TopicPartition {PausedTopicPartition} is currently paused.");
            }

            if (_latestConsumedTopicPartition != null
                && _latestConsumedTopicPartitionConsecutiveCount >= MaxConsecutiveMessagesPerPartition
                && consumer.IsAssigned(_latestConsumedTopicPartition))
            {
                // Remember which TopicPartition we Paused so that we can Resume it shortly:
                _pausedTopicPartition_inSingleElementArray[0] = _latestConsumedTopicPartition;

                _timeSinceLastCallToConsumer.Stop();

                _pauseCallTimer.Start();
                consumer.Pause(_pausedTopicPartition_inSingleElementArray);
                _pauseCallTimer.Stop();
                _pauseCallCount++;

                _timeSinceLastCallToConsumer.Restart();

                // Now delete our record of the "last consumed" TopicPartition and its consecutive count because
                // we don't want to keep Pausing this on every iteration of the loop:
                _latestConsumedTopicPartition = null;
                _latestConsumedTopicPartitionConsecutiveCount = 0;
            }
        }

        private void ResumePausedTopicPartition(IConsumer<TKey, TValue> consumer)
        {
            // If there is a Paused TopicPartition, then Resume it. This was Paused prior to Consume() being called. We only
            // need to keep it Paused for that one call to Consume(), because that will cause IConsumer to move to the next
            // Topic/Partition, at which point the Paused TopicPartition will be at the end of the line. So it's now safe to
            // Resume that TopicPartition, because even if it's not Paused the IConsumer will now try every other
            // TopicPartition first.

            if (_pausedTopicPartition_inSingleElementArray[0] != null)
            {
                _timeSinceLastCallToConsumer.Stop();

                _resumeCallTimer.Start();
                consumer.Resume(_pausedTopicPartition_inSingleElementArray);
                _resumeCallTimer.Stop();
                _resumeCallCount++;

                _timeSinceLastCallToConsumer.Restart();

                _pausedTopicPartition_inSingleElementArray[0] = null;
            }
        }

        [DebuggerDisplay("{DebuggerDisplay,nq}")]
        internal class Acknowledgement : IAcknowledgement
        {
            public int TotalMessageCount { get; private set; } = -1;
            public int AckCount { get; private set; }
            internal int CommitCount { get; set; }
            internal int NotYetCommittedAckCount => AckCount - CommitCount;
            internal bool HasUncommittedAckedMessages => NotYetCommittedAckCount > 0;
            internal bool IsInitialized => TotalMessageCount != -1;

            internal Acknowledgement()
            {
            }

            internal void Init(int messageCount)
            {
                if (messageCount <= 0)
                    throw new ArgumentException($"{nameof(Acknowledgement)}.{nameof(Init)}: messageCount must be greater than zero.");
                Reset();
                TotalMessageCount = messageCount;
            }

            public void Ack(int count)
            {
                if (TotalMessageCount < 0)
                    throw new InvalidOperationException($"{nameof(Acknowledgement)}.{nameof(Ack)}: " +
                        $"This Acknowledgement has not been initialized (after being instantiated or Reset), and thus cannot yet be used.");
                if (count < 0)
                    throw new ArgumentOutOfRangeException($"{nameof(Acknowledgement)}.{nameof(Ack)}: " +
                        $"Invalid count {count}, cannot be negative.");
                if (count > TotalMessageCount)
                    throw new ArgumentOutOfRangeException($"{nameof(Acknowledgement)}.{nameof(Ack)}: " +
                        $"Passed count {count} is greater than the total number of messages in the batch: {TotalMessageCount}.");
                if (count < AckCount)
                    throw new ArgumentOutOfRangeException($"{nameof(Acknowledgement)}.{nameof(Ack)}: " +
                        $"Passed count {count} is less than the previous Ack count of {AckCount}. This count can never be decreased.");

                AckCount = count;
            }

            internal void Reset()
            {
                // Allows this object to be reused.
                TotalMessageCount = -1;
                AckCount = 0;
                CommitCount = 0;
            }

            private string DebuggerDisplay => $"{nameof(Acknowledgement)}: Inited: {IsInitialized}, Acked: {AckCount}, Committed: {CommitCount}, " +
                $"Total: {TotalMessageCount}, Has uncommitted: {HasUncommittedAckedMessages}, Uncommitted: {NotYetCommittedAckCount}";
        }

        private int CommitAckedMessages(IConsumer<TKey, TValue> consumer)
        {
            if (!(_acknowledgement?.IsInitialized ?? false) || _activeBatch?.Count == 0)
                return 0; // _acknowledgement is null or uninitialized, or _activeBatch is null or empty.
            if (_acknowledgement.CommitCount == _acknowledgement.AckCount)
                return 0; // nothing to Commit, we're caught up.

            List<ConsumeResult<TKey, TValue>> activeBatch = _activeBatch;
            Acknowledgement acknowledgement = _acknowledgement;
            int startingCommitCount = acknowledgement.CommitCount;
            int ackCount = acknowledgement.AckCount;

            _commitTimer.Start();
            try
            {
                int numberCommitted = 0;
                for (int i = startingCommitCount; i < ackCount; i++)
                {
                    var message = activeBatch[i];

                    bool isInAssignedPartition = consumer.IsAssigned(message.TopicPartition);
#if DEBUG
                    if (!isInAssignedPartition)
                    {
                        try
                        {
                            string consumerName = consumer.Name;
                            bool handleInvalid = consumer.Handle.IsInvalid;
                            Console.WriteLine($"Consumer '{Name}': Committing message in TopicPartition {message.TopicPartition}, which is not assigned to " +
                                $"our IConsumer {consumer}, name: {consumerName}, handle invalid: {handleInvalid}, handle: {consumer.Handle}, " +
                                $"group metadata: {consumer.ConsumerGroupMetadata}");
                        }
                        catch (Exception ex)
                        {
                            Console.Error.WriteLine($"Consumer '{Name}': Committing message in TopicPartition {message.TopicPartition}, which is not assigned to our IConsumer. " +
                                $"And errored attempting to read data for IConsumer: {ex}");
                        }
                    }
#endif

                    // Record in our Acknowledgement that we're Committing this Message. Do this prior to calling
                    // IConsumer's StoreOffset(), due to a fear that StoreOffset() might call our Partition Revocation
                    // Handler, which in turn might call this same method to Commit Messages. I don't want that to cause
                    // a double Commit (though that may not be a bad thing). Update: I don't think that StoreOffset()
                    // can cause the Partition Revocation Handler to be invoked, but this is still okay to do.
                    acknowledgement.CommitCount = i + 1;

                    _timeSinceLastCallToConsumer.Stop();
                    // Note: For this to work correctly, "enable.auto.commit" must be true and "enable.auto.offset.store"
                    // must be false. Please see the comments in the constructor where it sets ConsumerConfig's
                    // EnableAutoCommit and EnableAutoOffsetStore.
                    // Note: I chose to use StoreOffset() instead of Commit() because then the actual Commit occurs in
                    // the background. If there are multiple instances of this running on the same CLR (and sharing the
                    // same Consumer Group), then the IConsumer can batch these commits.
                    // I learned the hard way: Don't mix calls to both StoreOffset() and Commit(). Do one or the other.
                    try
                    {
                        consumer.StoreOffset(message);
                    }
                    catch (Exception)
                    {
                        // Just for debugging.
                        throw;
                    }
                    _timeSinceLastCallToConsumer.Restart();
                    numberCommitted++;

                    _commitOffsetCount++;
                    _totalCountOfAckedMessages++;

                    // I'm not sure, but it's possible that the above call to IConsumer's StoreOffset() *might* call our
                    // Partition Revocation Handler if a rebalance has been requested by the Kafka Broker. If that
                    // happens, our Handler might run this same method to Seek Nack'd Messages, and then afterward it
                    // would null out _activeTask and reset _activeBatch & _acknowledgement. When it clears _activeBatch
                    // that will cause a modification error with the iterator that we're using, so we need to break out
                    // of this loop immediately to avoid that error:
                    if (!(_acknowledgement?.IsInitialized ?? false) || _activeBatch?.Count == 0)
                        break; // _acknowledgement is null or uninitialized, or _activeBatch is null or empty.
                }
                return numberCommitted;
            }
            finally
            {
                _commitTimer.Stop();
            }
        }

        private void SeekBackToAllNackedMessages(IConsumer<TKey, TValue> consumer)
        {
            if (!(_acknowledgement?.IsInitialized ?? false) || _activeBatch?.Count == 0)
                return; // _acknowledgement is null or uninitialized, or _activeBatch is null or empty.
            if (_acknowledgement.AckCount >= _activeBatch.Count)
                return; // then every message was Ack'd, nothing needs to be Nack'd.

            var acknowledgement = _acknowledgement;
            var activeBatch = _activeBatch;

            // To "Nack" a message in Kafka, we technically don't need to do anything as far as the Kafka Broker's Consumer
            // Group is concerned: Simply not advancing the Consumer Group's Offset (for this message's TopicPartition) will
            // cause the message to be re-delivered again in the future. However, the IConsumer stores its own in-memory
            // buffer (per TopicPartition) because it needs to remember which messages it has returned from its Consume()
            // method. So we need to instruct the IConsumer to do an in-memory "Seek" back to the earliest ConsumeResult that
            // we want to Nack (per TopicPartition), which will cause this (and all subsequent ConsumeResults in the same
            // TopicPartition) to be re-delivered by the next calls to IConsumer's Consume() method. For each TopicPartition,
            // we only need to Seek() to the message with the lowest Offset. Unlike our Commit logic, we don't need to update
            // the Acknowledgement object as we go, so no need to do this in order of the messages in _activeBatch.

            // Get an IEnumerable that selects every Nack'd message by skipping the Ack'd messages:
            var nackedMessages = activeBatch.Skip(acknowledgement.AckCount);
            _totalCountOfNackedMessages += SeekBackTo(consumer, nackedMessages);
        }

        private long SeekBackTo(IConsumer<TKey, TValue> consumer, IEnumerable<ConsumeResult<TKey, TValue>> messagesToSeekTo)
        {
            long count = 0;

            // The passed messagesToSeekTo List might contain messages in many different TopicPartitions. Also, many of the
            // messages might share the same TopicPartitions. For each TopicPartition, we only need to Seek back to the
            // message with the lowest Offset. Group by each ConsumeResult by its TopicPartition:
            var groupsByTopicPartition = messagesToSeekTo.GroupBy(m => m.TopicPartition);
            foreach (var messagesInTopicPartition in groupsByTopicPartition)
            {
                // For Nack'd messages sharing this TopicPartition, find the message with the LOWEST Offset. That's what
                // we'll Seek() back to. Good news: These Messages are ordered in the original _activeBatch list by their
                // Offset (but this order only applies relative to other messages with the same TopicPartition, which is what
                // we're dealing with here). So we simply need to select the first message to get the lowest Offset:
                var messageWithLowestOffsetToNack = messagesInTopicPartition.First();
                count += messagesInTopicPartition.Count();

                SeekBackTo(consumer, messageWithLowestOffsetToNack);
            }
            return count;
        }

        private void SeekBackTo(IConsumer<TKey, TValue> consumer, ConsumeResult<TKey, TValue> messageToSeekTo)
        {
            if (!consumer.IsAssigned(messageToSeekTo.TopicPartition))
                return; // If this TopicPartition isn't assigned, the below would throw error: Local: Erroneous state.

            _timeSinceLastCallToConsumer.Stop();

            _seekTimer.Start();
            consumer.Seek(messageToSeekTo.TopicPartitionOffset);
            _seekTimer.Stop();
            _seekCount++;

            _timeSinceLastCallToConsumer.Restart();
        }

        private IEnumerable<TopicPartitionOffset> PartitionsAssignedHandler(IConsumer<TKey, TValue> consumer,
            List<TopicPartition> assignedList)
        {
            // We don't need to implement this Partition Assignment Handler. For now it's here for debugging purposes.
            //#if DEBUG
            //            string infoAbout(TopicPartition assignedTp)
            //            {
            //                int numberInBuffer = _activeBatch?.Count(cr => cr.TopicPartition == assignedTp) ?? 0;
            //                string alreadyInBuffer = numberInBuffer > 0 ? $" (ALREADY IN BUFFER: {numberInBuffer})" : "";
            //                return $"{assignedTp}{alreadyInBuffer}";
            //            }
            //            Console.WriteLine($"+ {Name} (tid {Environment.CurrentManagedThreadId}): " +
            //                $"{assignedList.Count} TopicPartitions Assigned " +
            //                $"{_runTimer.Elapsed.TotalSeconds:n3} seconds after starting, " +
            //                $"adding to {consumer.Assignment.Count} previously assigned:\n" +
            //                $"  {string.Join("\n  ", assignedList.Select(infoAbout))}\n");
            //            //lock (cStaticDebugMonitor)
            //            //{
            //            //    Debug.WriteLine("+++++++++++++++++");
            //            //    Debug.WriteLine($"{Name}: Assigning TopicPartitions:\n" +
            //            //        $"{string.Join($"\n+  {Name}: ", assignedList.Select(infoAbout))}");
            //            //}
            //#endif
            // Important: We must return all of the TopicPartitions in assignedList, otherwise we'd reject these assignments.
            // But we need to convert TopicPartitions to TopicPartitionOffsets. We can set all of these Offsets to Unset:
            return assignedList.Select(tp => new TopicPartitionOffset(tp, Offset.Unset));
        }

        private IEnumerable<TopicPartitionOffset> PartitionsRevokedHandler(IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> revokedList)
        {
            try
            {
                return _PartitionsRevokedHandler(consumer, revokedList);
            }
            catch (Exception ex)
            {
                // This Partition Revocation Handler should not throw an exception. This has to complete so that the
                // revocation can finish. If there's an exception, order a Shutdown and store the exception, which will be
                // thrown by RunSynchronously() (unless another exception has already been stored).
                if (!ex.IsOperationCancelled()) // ignore OperationCancelledException, even if it's inside an AggregateException.
                {
                    // This exception from the processor should shutdown our consumer. This stores the exception so that
                    // it can be thrown by our RunSynchrnously method.
                    Shutdown(ExceptionDispatchInfo.Capture(ex));
                }
            }
            // We don't need to return anything. We already handled Committing and Seeking above. Fun fact: After version 1.5
            // of Confluent.Kafka, this return result is completely ignored. "The goggles, they do nothing!"
            return Enumerable.Empty<TopicPartitionOffset>();
        }

        private IEnumerable<TopicPartitionOffset> _PartitionsRevokedHandler(IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> revokedList)
        {
            // This is called when some of our TopicPartitions are being revoked. This happens synchronously when
            // IConsumer.Consume() is invoked, so we don't need to worry about concurrent access to instance variables. This
            // is also called when IConsumer.Close() is called. Revocation won't occur until after this method returns. This
            // method has the opportunity to Commit Offsets for any of the revoked TopicPartitions by returning updated
            // TopicPartitionOffsets (after reviewing the Confluent.Kafka code, I don't think the return result works).
#if DEBUG
            try
            {
                string infoAbout(TopicPartitionOffset tpo)
                {
                    string additionalInfo = string.Empty;
                    int numberInBuffer = _activeBatch?.Count(cr => cr.TopicPartition == tpo.TopicPartition) ?? 0;
                    if (numberInBuffer > 0)
                        additionalInfo += $" (in current batch: {numberInBuffer})";
                    if (tpo.TopicPartition == PausedTopicPartition)
                        additionalInfo += $" (paused)";
                    if (tpo.TopicPartition == _latestConsumedTopicPartition)
                        additionalInfo += $" (streak: {_latestConsumedTopicPartitionConsecutiveCount})";
                    return $"{tpo}{additionalInfo}";
                }
                string msg = $"> {Name} (tid {Environment.CurrentManagedThreadId}): " +
                    $"Revoking {revokedList.Count} of {consumer.Assignment.Count} Partitions, " +
                    $"{_runTimer.Elapsed.ToStringPretty()} after starting";
                if (revokedList.Count > 0)
                    msg += $":\n    - {string.Join("\n    - ", revokedList.Select(infoAbout))}";
                Console.WriteLine(msg);
                //Console.WriteLine($"- {Name} (tid {Environment.CurrentManagedThreadId}): " +
                //    $"Revoking {revokedList.Count} of {consumer.Assignment.Count} Partitions, " +
                //    $"{_runTimer.Elapsed.TotalSeconds:n3} seconds after starting:\n" +
                //    $"  {string.Join("\n  ", revokedList.Select(infoAbout))}\n");
                //lock (cStaticDebugMonitor)
                //{
                //    Debug.WriteLine("~~~~~~~~~~~~~~~~~~~~~~~");
                //    //Debug.WriteLine($"{Name}: Revoking TopicPartitions:");
                //    //foreach (var revokedTpo in revokedList)
                //    //{
                //    //    int numberInBuffer = _consumeResults.Count(cr => cr.TopicPartition == revokedTpo.TopicPartition);
                //    //    Debug.WriteLine($"-  {Name}: {revokedTpo}{(numberInBuffer > 0 ? $"  (in buffer: {numberInBuffer})" : "")}");
                //    //}

                //    if (_activeTask != null)
                //    {
                //        var batch = _activeBatch;
                //        if (batch?.Count > 0)
                //        {
                //            Debug.WriteLine($"{Name}: Actively processing TopicPartitions being revoked:");
                //            var topicPartitionsGrouped = batch.GroupBy(cr => cr.TopicPartition);
                //            int countOfBufferedPartitionsBeingRevoked = 0;
                //            foreach (var group in topicPartitionsGrouped)
                //            {
                //                var revokedTpo = revokedList.FirstOrDefault(tpo => tpo.TopicPartition == group.Key);
                //                if (revokedTpo != null)
                //                {
                //                    countOfBufferedPartitionsBeingRevoked++;
                //                    Debug.WriteLine($"-  {Name}: {countOfBufferedPartitionsBeingRevoked}. {group.Key}: " +
                //                        $"Buffered {group.Count()} from {group.First().TopicPartitionOffset} " +
                //                        $"to {group.Last().TopicPartitionOffset}. Revoked: {revokedTpo}");
                //                }
                //            }
                //        }
                //    }

                //    //OutputStatsToDebug();
                //    Debug.WriteLine("~~~~~~~~~~~~~~~~~~~~~~~");
                //}
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error writing debug info to console:\n{ex}");
            }
#endif

            bool isTopicPartitionBeingRevoked(TopicPartition? topicPartition)
            {
                return topicPartition != null && revokedList.Any(revoked => revoked.TopicPartition == topicPartition);
            }
            bool isBeingRevoked(ConsumeResult<TKey, TValue>? consumeResult)
            {
                return isTopicPartitionBeingRevoked(consumeResult?.TopicPartition);
            }

            // 1. Does _activeBatch contain any messages belonging to one of the TopicPartitions that are being revoked,
            // or any messages belonging to TopicPartitions that are no longer in this IConsumer's Assigned list?
            bool activeBatchContainsRevokedMessages = _activeBatch?
                .Any(cr => isBeingRevoked(cr) || !consumer.IsAssigned(cr.TopicPartition)) ?? false;

            // 2. If there's a non-null _activeTask, decide if it should be cancelled. If this is a normal Partition
            //    Revocation then only cancel it if one of the messages being processed (in _activeBatch) belongs to a
            //    revoked TopicPartition. But if this is called after Shutdown has been ordered (perhaps it's being
            //    called from within IConsumer's Close()) then always cancel the _activeTask.
            bool cancelActiveTask = _activeTask != null && (activeBatchContainsRevokedMessages || ShutdownOrdered);
            if (cancelActiveTask)
            {
#if DEBUG
                Console.WriteLine($"$$$$$$ {Name} (tid {Environment.CurrentManagedThreadId}): Cancelling active processor " +
                    $"with {_acknowledgement?.AckCount} of {_activeBatch?.Count} messages processed ({_acknowledgement?.CommitCount} committed), " +
                    $"{_activeTaskTimer?.Elapsed.ToStringPretty()} after starting.");
#endif
                // 2a. Cancel our active processor's CancellationToken, which tells the processor code to gracefully stop
                // processing as soon as possible. The processor is allowed to keep working if it's beyond the point of no
                // return, but it should try to avoid starting any messages that aren't already underway (this is what we
                // call a "graceful cancellation/shutdown"). The processor may continue to Ack any messages that it finishes,
                // which we'll Commit below. Any un-Ack'd Messages (aka Nack'd) will be Seeked below.
                _abortProcessingCancellationSource?.Cancel();
                // Below, we'll wait for this Task to finish. But first we'll perform some other work while we wait; we just
                // wanted to fire this cancellation as early as possible.
            }

            try
            {
                // 3. If we've currently Paused a TopicPartition that's being revoked then Resume it immediately. Note: This
                //    might not seem necessary, because if this TopicPartition is assigned to a different Consumer, then that
                //    separate IConsumer won't have it Paused. But due to weirdness (described in comments elsewhere),
                //    sometimes a revoked TopicPartition stays with our current IConsumer, and all of the state for that
                //    TopicPartition remains intact, including its Paused state. Therefore, we need to Resume.
                if (PausedTopicPartition != null
                    && (ShutdownOrdered || isTopicPartitionBeingRevoked(PausedTopicPartition)))
                {
                    ResumePausedTopicPartition(consumer);
                }
            }
            catch (Exception ex)
            {
                // If an exception is thrown, we still want to execute the code below which handles the _activeTask.
                // So don't throw this out of this method.
                if (!ex.IsOperationCancelled()) // ignore OperationCancelledException, even if it's inside an AggregateException.
                {
                    // This exception from the processor should shutdown our consumer. This stores the exception so that
                    // it can be thrown by our RunSynchrnously method (unless there's already a stored exception).
                    Shutdown(ExceptionDispatchInfo.Capture(ex));
                }
            }

            // 4. If the "latest to be consumed" TopicPartition is being revoked then stop keeping track of it:
            if (_latestConsumedTopicPartition != null && isTopicPartitionBeingRevoked(_latestConsumedTopicPartition))
            {
                _latestConsumedTopicPartition = null;
                _latestConsumedTopicPartitionConsecutiveCount = 0;
            }

            // 5. If the _activeTask processor is being cancelled then we need to wait for it to finish and then handle any
            //    Ack'd/Nack'd messages prior to this method ending. We triggered the CancellationToken in Step #1, then we
            //    did other work in the Steps above, giving that task some time to gracefully end. Note: The Partition
            //    Revocation won't actually occur (in the Consumer Group) until this method ends, so we are able to make last
            //    minute Commits prior to losing access to these TopicPartitions. Note: In the comments below I describe some
            //    "funny business" with the Confluent.Kafka library: It has the bad habit of revoking every TopicPartition
            //    and then immediately re-assigning some of the TopicPartitions. When that happens, our IConsumer's state
            //    doesn't change, so any Offsets that it stored in-memory to remember what it has returned from Consume()
            //    won't change after the immediate Revoke+Assignment, nor will the Paused state of those TopicPartitions.
            //    This is why the code below goes to extra lengths to call IConsumer methods like Seek() and Resume(), which
            //    normally wouldn't be necessary if the Revoked TopicPartition was being assigned to a totally separate
            //    IConsumer.
            if (cancelActiveTask)
            {
                // 5a. Wait for the _activeTask to finish. We need a sanity timeout because if we wait too long then our
                // heartbeat with the Consumer Group will expire and we'll lose all of our TopicPartitions. Or if _isClosing
                // == true and we wait too long, we might exceed the amount of time that Kubernetes (or our Azure WebJob) has
                // allotted for us to gracefully shutdown.

                //  var waitForTaskCompletionTimer = Stopwatch.StartNew();

                // How long should we wait? Well, that depends on the reason for this shutdown. First, there's the max amount
                // of time we should wait before the Kafka Broker's Consumer Group will consider us lapsed and revoke our
                // TopicPartitions. That would be bad, because then we won't be able to Commit the messages that the
                // _activeTask processor has Ack'd (in which case they'll be processed again by another consumer process).
                int maxWaitMillis = _maxMillisecondsToWaitForAbortedTaskToFinish;
                if (ShutdownOrdered)
                {
                    // If we were ordered to shutdown, then there's another consideration: We assume that our host has
                    // told us we need to end, perhaps because our container is about to be terminated. There's typically
                    // a "grace peroid" to end, and we don't want to exceed that.
                    maxWaitMillis = Math.Min(maxWaitMillis, ShutdownGracePeriodInMilliseconds);
                }
                HandleAcknowledgementsAndTryToFinishActiveTask(consumer, maxWaitMillis);
                // Note: If _activeTask Faulted then this method will throw that exception, after Ack'ing and Nack'ing.
            }
            else if (_activeTask == null && activeBatchContainsRevokedMessages)
            {
                // 6. Then there is no _activeTask, but _activeBatch contains messages and some of them belong to a
                //    TopicPartition that's being revoked. Here's how this can happen: ConsumeMultiple will call Consume()
                //    many times, filling up _activeTask as it goes. On one of those calls to Consume(), this Partition
                //    Revocation Handler was invoked. Obviously, we need to remove any messages that are already in
                //    _activeBatch which belong to a revoked TopicPartition, because otherwise those will be passed to a new
                //    _activeTask later (if that happens, a "Local: Erroneous State" error will occur when we try to Commit
                //    it by calling StoreOffset()). Because there is not yet any _activeTask, none of these messages were
                //    proccesed, so we're going to Nack all of them. Even though the Partition is being revoked, we still
                //    need call Seek() because of a known flaw with IConsumer: It sometimes passes every TopicPartition to
                //    this, and immediately re-assigns them to this same IConsumer. When it does that, it keeps its in-memory
                //    state for that TopicPartition. That's why we need to tell it to Seek() back to these messages so that
                //    they can be redelivered.
                List<ConsumeResult<TKey, TValue>> messagesInRevokedTopicPartitions_copy = _activeBatch.Where(isBeingRevoked).ToList();
                // Tell the IConsumer to Seek back to the earliest message per TopicPartition:
                SeekBackTo(consumer, messagesInRevokedTopicPartitions_copy);
                // Remove these messages from _activeBatch:
                _activeBatch.RemoveAll(cr => messagesInRevokedTopicPartitions_copy.Contains(cr));
            }

            // We don't need to return anything. We already handled Committing and Seeking above. Fun fact: After version 1.5
            // of Confluent.Kafka, this return result is completely ignored. "The goggles, they do nothing!"
            return Enumerable.Empty<TopicPartitionOffset>();
        }

        private IEnumerable<TopicPartitionOffset> PartitionsLostHandler(IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> lostList)
        {
#if DEBUG
            Console.WriteLine($"- {Name} (tid {Environment.CurrentManagedThreadId}): Partitions LOST!");
#endif
            return PartitionsRevokedHandler(consumer, lostList);
        }

        protected virtual void ErrorHandler(IConsumer<TKey, TValue> consumer, Error error)
        {
            // TODO: Log this error.
            Debug.WriteLine(error);
        }

        private void ResetActiveBatchAndTask(bool nullEverythingInsteadOfReusing = false)
        {
            _activeTask = null;
            _activeTaskTimer = null;
            _activeTaskCompletedToken = null;
            _abortProcessingCancellationSource?.Dispose();
            _abortProcessingCancellationSource = null;
            if (nullEverythingInsteadOfReusing)
            {
                // Dereference the Acknowledgement object that was passed to the processor. A new one will be allocated next
                // time:
                _acknowledgement = null;
                // Dereference the List instance that was passed to the processor. A new one will be allocated next time.
                _activeBatch = null;
                // Also null the ReadOnlyList that contains _activeBatch. If we don't do this then we'd keep sending the same
                // messages to each processor (very bad):
                _activeBatchReadOnly = null;
            }
            else
            {
                // It's safe to simply re-use the Aknowledgement and List instances, so clear them. This improves memory
                // efficiency, because we don't need to instantiate a new List each time. But this means that sub-classes
                // that implement a processor cannot hang onto a List and IAcknowledgement after they are done with them.
                _acknowledgement?.Reset(); // this will need to have Init(int) called before it can be used.
                _activeBatch?.Clear();
                // Leave _activeBatchReadOnly as-is, because it reads-through to _activeBatch.
            }
        }

        /// <summary>
        /// This attempts to mimic how the Java Kafka Consumer behaves. It attempts to consume a batch of messages, putting
        /// each into the passed sinkList. It will wait for timeoutMilliseconds for the first message. After that, it will
        /// only consume additional messages if they're ready-and-waiting, it won't wait after adding a message to the
        /// sinkList. This will return when one of the following conditions are met: 1. It waited timeoutMilliseconds and
        /// didn't consume any messages. 2. It added maxMessageCount messages to sinkList. 3. It added at least one message
        /// to sinkList, and there are no more messages immediately available. Note: This can consume messages from multiple
        /// Topics and Partitions.
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="maxMessageCount"></param>
        /// <param name="sinkList">If this is non-null, then each consumed message to this List, and this List will be
        /// returned by this method. If this is null then a new List will be automatically created to contain consumed
        /// messages, and returned. This accepts an optional List because the caller might wish to re-use an existing List to
        /// avoid lots of memory allocations and garbage collection. If this List is non-null, this doesn't care if there are
        /// already messages in it (though whether that makes sense is a concern for your business logic).</param>
        /// <param name="timeoutMilliseconds">How long (in milliseconds) this will wait for the first message. If this time
        /// is exceeded and no messages are consumed, this will return zero.</param>
        /// <param name="cancelToken">When this CancellationToken is cancelled this will abort immediately and throw an
        /// OperationCanceledException.</param>
        /// <returns>The number of messages that were consumed and added to sinkList.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="ArgumentException"></exception>
        private void ConsumeMultiple(IConsumer<TKey, TValue> consumer, int maxMessageCount, List<ConsumeResult<TKey, TValue>>? sinkList,
            CancellationToken stopListeningToken, int timeoutMilliseconds, CancellationToken externalAbortToken)
        {
            if (maxMessageCount < 0)
                throw new ArgumentException($"Invalid {nameof(maxMessageCount)}: {maxMessageCount}", nameof(maxMessageCount));
            if (maxMessageCount > 0 && sinkList == null)
                throw new ArgumentNullException(nameof(sinkList)); // can only be null is maxMessageCount == 0

            CancellationToken comboCancelToken = CancellationToken.None;
            if (timeoutMilliseconds > 0)
            {
                var cancelSource = CancellationTokenSource.CreateLinkedTokenSource(stopListeningToken, externalAbortToken);
                cancelSource.CancelAfter(timeoutMilliseconds);
                comboCancelToken = cancelSource.Token;
            }

            int consumedCount = 0;
            do
            {
                try
                {
                    _timeSinceLastCallToConsumer.Stop();

                    ConsumeResult<TKey, TValue>? message;
                    if (timeoutMilliseconds <= 0)
                    {
                        // Tell Consume() not to wait for the next message
                        // (it will only return what it has already buffered):
                        message = consumer.Consume(0);
                    }
                    else
                    {
                        message = consumer.Consume(comboCancelToken);
                    }
                    _timeSinceLastCallToConsumer.Restart();

                    if (message == null)
                    {
                        // No more messages found within the time allotted.
                        break;
                    }
                    // If we're here then result is non-null.

                    if (maxMessageCount == 0)
                    {
                        // If maxMessageCount is zero then all we needed to do was call Consume() once. Since the caller
                        // doesn't want any Messages back, we need to "put back" this Message by calling Seek() (and we won't
                        // be adding it to the passed sinkList, nor will we be incrementing the count variable).
                        SeekBackTo(consumer, message);
                        // We're done. Don't add this to sinkList (because we put it back) and don't increment count.
                        // This will return zero.
                        break;
                    }

                    //sinkList ??= new(maxMessageCount);
                    sinkList.Add(message);
                    consumedCount++;

                    // Now that we've retrieved at least one Message, set the timeout to zero so that we don't wait for any
                    // additional Messages. We'll still call Consume() to attempt to get more Messages, but with no timeout
                    // the IConsumer will either need to give us a Message already pre-fetched, or return immediately.
                    timeoutMilliseconds = 0;
                }
                catch (ConsumeException cx)
                {
                    if (cx.Error.Code == ErrorCode.UnknownTopicOrPart)
                    {
                        // Then no matching Topic exists. We hope that a matching Topic will be created in the near future.
                        // We want the caller to keep calling this until a Topic appears. Note: The Consumer will only throw
                        // this exception the first time we call Consume(). For every subsequent Consume() call it won't
                        // throw this, rather it will just wait for the timeout hoping that a Topic/Partition is assigned.
                        break;
                    }
                }
                catch (OperationCanceledException ocex)
                {
                    // If this was cancelled because the passed externalAbortToken caused this exception then re-throw it
                    // (propagate it up the stack).
                    if (externalAbortToken.IsCancellationRequested)
                        throw;
                    // Otherwise this was cancelled only because stopListeningToken was cancelled or because
                    // timeoutMilliseconds was exceeded, in which case we'll just break of out this loop and return normally.
                    break;
                }
            }
            while (consumedCount < maxMessageCount);
        }

        private void DelaySynchronouslyDoNotThrowException(int timeoutMilliseconds, CancellationToken cancelToken1, CancellationToken cancelToken2)
        {
            if (timeoutMilliseconds <= 0 || cancelToken1.IsCancellationRequested || cancelToken2.IsCancellationRequested)
                return;

            _delayTimer.Start();
            _delayCount++;

            var cancelSource = CancellationTokenSource.CreateLinkedTokenSource(cancelToken1, cancelToken2);
            try
            {
                // *Synchronously* wait until timeout has elapsed or the combined cancelSource. Token is cancelled:
                Task.Delay(timeoutMilliseconds, cancelSource.Token).GetAwaiter().GetResult();
            }
            catch (OperationCanceledException)
            {
                // Then one of our two CancellationTokens was cancelled. We don't throw an exception when that happens.
                // Presumably the caller is already coded to check those CancellationTokens and behave accordingly.
            }
            _delayTimer.Stop();
        }

        private void CollectStats(IConsumer<TKey, TValue> consumer, IEnumerable<ConsumeResult<TKey, TValue>>? messages)
        {
            if (_timeSinceLastWatermarkCollection.Elapsed.TotalSeconds < cWatermarkStatCollectionIntervalInSeconds)
                return;

            try
            {
                foreach (var topicPartition in consumer.Assignment)
                {
                    ConsumeResult<TKey, TValue>? lastMatchingMessage = messages?.LastOrDefault(cr => cr.TopicPartition == topicPartition);

                    TopicPartitionWatermarkInfo? watermarkInfo;
                    if (_topicPartitionWatermarkInfos.TryGetValue(topicPartition, out watermarkInfo))
                    {
                        // Then we've previously recorded watermark info for this topicPartition.

                        if ((DateTimeOffset.Now - watermarkInfo.GeneratedAt).TotalMinutes >= cMaxWatermarkStatRetentionTimeInMinutes)
                        {
                            _topicPartitionWatermarkInfos.Remove(topicPartition, out _);
                            watermarkInfo = null;
                        }
                        else if (lastMatchingMessage == null && watermarkInfo.Current != Offset.Unset)
                        {
                            // Then we don't have a matching message in the passed messages list, which means that we don't
                            // know the current Offset for this topicPartition. If the previously recorded watermark info
                            // DOES contain a valid Offset, then we don't want to overwrite it with new watermark info that
                            // DOESN'T have an Offset.
                            continue; // skip ahead to the next TopicPartition.
                        }
                    }

                    // If we're here, then we want to get fresh watermark data for this topicPartition.

                    // See if we have a current message in this TopicPartition, so that we can determine its current Offset:
                    //ConsumeResult<TKey, TValue>? message = messages.LastOrDefault(cr => cr.TopicPartition == topicPartition);

                    var watermarks = consumer.GetWatermarkOffsets(topicPartition);
                    if (watermarks != null)
                    {
                        watermarkInfo = new TopicPartitionWatermarkInfo(topicPartition,
                            watermarks ?? new WatermarkOffsets(Offset.Unset, Offset.Unset),
                            lastMatchingMessage?.Offset ?? Offset.Unset,
                            AverageMessageProcessingTime);
                        _topicPartitionWatermarkInfos.AddOrUpdate(topicPartition, watermarkInfo, (k, v) => watermarkInfo);
                    }
                }
                _timeSinceLastWatermarkCollection.Restart();
            }
            catch (Exception ex)
            {
                // Since this is stat collection, no error should be thrown.
                // TODO: Log this!
            }
        }

        private static int? Min(params int?[] numbers)
        {
            int? lowest = null;
            foreach (var number in numbers)
            {
                if (number != null && (lowest == null || number < lowest))
                {
                    lowest = number;
                }
            }
            return lowest;
        }

        public string GetStats()
        {
            StringWriter writer = new();
            WriteStats(writer);
            return writer.ToString();
        }

        public void WriteStats(TextWriter writer)
        {
            writer.Write($"Consumer '{Name}'");
            if (Running)
                writer.WriteLine($" has been running for {Runtime.ToStringPretty()}.");
            else if (FatalException != null)
                writer.WriteLine($" faulted after running for {Runtime.ToStringPretty()}, error: {FatalException?.Message}.");
            else if (Finished)
                writer.WriteLine($" finished, ran for {Runtime.ToStringPretty()}.");
            else
                writer.WriteLine(" not yet started.");

            var batch = _activeBatch;
            if (batch?.Count > 0)
            {
                var groupedByTopicPartition = batch.GroupBy(cr => cr.TopicPartition);
                writer.WriteLine($"  {nameof(batch)} count={batch?.Count}, " +
                    $"acked={_acknowledgement?.AckCount} (committed={_acknowledgement?.CommitCount}), " +
                    $"TopicPartition breakdown (distinct count: {groupedByTopicPartition.Count()}):");
                writer.WriteLine($"    {string.Join(", ", groupedByTopicPartition.Select(g => $"{g.Key}: {g.Count()}"))}");
            }

            writer.WriteLine($"  Latest consumed TopicPartition (not yet Paused): " +
                $"{_latestConsumedTopicPartition}, consecutive count: {_latestConsumedTopicPartitionConsecutiveCount}");

            if (_pausedTopicPartition_inSingleElementArray[0] == null)
            {
                writer.WriteLine($"  No Paused TopicPartition.");
            }
            else
            {
                writer.WriteLine($"  Paused TopicPartition: {_pausedTopicPartition_inSingleElementArray[0]}");
            }

            //long totalLoopCount = _loopCountWhenMessageRetrieved + _loopCountWhenNothingConsumed;
            double averageMessagePerConsumeBatch = _countOfConsumptionBatchesWithAtLeastOneMessage != 0
                ? (double)_totalCountOfConsumedMessages / (double)_countOfConsumptionBatchesWithAtLeastOneMessage : 0;
            double averageMessagesPerProcessorBatch = _processorTaskCount != 0
                ? (double)_totalCountOfConsumedMessages / (double)_processorTaskCount : 0;

            writer.WriteLine($"  Throughput: {ProcessedMessageThroughputPerSecond:n2} messages/second. Non-idle: {NonIdleThroughputPerSecond:n2} messages/second.");
            writer.WriteLine($"    each message averaged {AverageMessageProcessingTime.ToStringPretty()}");
            writer.WriteLine($"  {_countOfConsumptionBatchesWithAtLeastOneMessage} successful ConsumeMultiples, " +
                $"average: {_consumeTimeWhenMessageRetrieved_excludingFirst.GetAverage(_countOfConsumptionBatchesWithAtLeastOneMessage - 1).ToStringPretty()} " +
                $"(per message: {_consumeTimeWhenMessageRetrieved_excludingFirst.GetAverage((_countOfConsumptionBatchesWithAtLeastOneMessage - 1) * averageMessagePerConsumeBatch).ToStringPretty()}), " +
                $"total (excluding first): {_consumeTimeWhenMessageRetrieved_excludingFirst}");
            writer.WriteLine($"    {_totalCountOfConsumedMessages} messages consumed, average per polled batch: {averageMessagePerConsumeBatch:n2}");
            //writer.WriteLine($"{_countOfConsumptionBatchesWithAtLeastOneMessageAndNoWait} " +
            //    $"successful ConsumeMultiples with no wait, " +
            //    $"average: {_consumeTimeNoWaitWhenMessageRetrieved.GetAverage(_countOfConsumptionBatchesWithAtLeastOneMessageAndNoWait).ToStringPretty()}, " +
            //    $"total: {_consumeTimeNoWaitWhenMessageRetrieved}");
            writer.WriteLine($"{_countOfConsumptionBatchesWithNoResults} empty ConsumeMultiples, " +
                $"average: {_consumeTimeWhenNoResults.GetAverage(_countOfConsumptionBatchesWithNoResults).ToStringPretty()}, total: {_consumeTimeWhenNoResults.ToStringPretty()}");
            writer.WriteLine($"{_commitOffsetCount} Offset Commits, average: {_commitTimer.Elapsed.GetAverage(_commitOffsetCount).ToStringPretty()}, total: {_commitTimer.Elapsed.ToStringPretty()}");
            writer.WriteLine($"{_pauseCallCount} Pauses, average time: {_pauseCallTimer.Elapsed.GetAverage(_pauseCallCount).ToStringPretty()}, total: {_pauseCallTimer.Elapsed.ToStringPretty()}");
            writer.WriteLine($"{_resumeCallCount} Resumes, average: {_resumeCallTimer.Elapsed.GetAverage(_resumeCallCount).ToStringPretty()}, total: {_resumeCallTimer.Elapsed.ToStringPretty()}");
            writer.WriteLine($"{_delayCount} Delays, average: {_delayTimer.Elapsed.GetAverage(_delayCount).ToStringPretty()}, total: {_delayTimer.Elapsed.ToStringPretty()}");
            writer.WriteLine($"{_processorTaskCount} Processor runs, average: {TimeSpan.FromTicks(_totalProcessingTimeInTicks).GetAverage(_processorTaskCount).ToStringPretty()}, " +
                $"total: {TimeSpan.FromTicks(_totalProcessingTimeInTicks)}");
            writer.WriteLine($"    average batch size: {averageMessagesPerProcessorBatch:n2}, " +
                $"highest batch size: {_highestProcessedBatchCount}, " +
                $"number of maxed-out batches: {_numberOfMaxedBatches}");
            //writer.WriteLine($"{_loopCountWhenMessageRetrieved} loops when message retrieved, average: {_loopTimeWhenMessageRetrieved.GetAverage(_loopCountWhenMessageRetrieved).ToStringPretty()}");
            //writer.WriteLine($"{_loopCountWhenNothingConsumed} loops when nothing consumed, average: {_loopTimeWhenNothingConsumed.GetAverage(_loopCountWhenNothingConsumed).ToStringPretty()}");
            //if (_consumerConfig != null)
            //{
            //    //writer.WriteLine($"fetch.min.bytes: {_consumerConfig.FetchMinBytes}");
            //    //writer.WriteLine($"fetch.wait.max.ms: {_consumerConfig.FetchWaitMaxMs}");
            //    //writer.WriteLine($"fetch.max.bytes: {_consumerConfig.FetchMaxBytes}");
            //    //writer.WriteLine($": {_consumerConfig.AutoCommitIntervalMs}");
            //    //writer.WriteLine($": {_consumerConfig}");
            //    //writer.WriteLine($": {_consumerConfig}");
            //    //writer.WriteLine($"auto.offset.reset: {_consumerConfig.AutoOffsetReset}");
            //    //writer.WriteLine($": {_consumerConfig}");
            //    //writer.WriteLine($": {_consumerConfig}");
            //    //writer.WriteLine($": {_consumerConfig}");
            //    //writer.WriteLine($": {_consumerConfig}");

            //    PropertyInfo[] properties = typeof(ConsumerConfig).GetProperties();
            //    foreach (PropertyInfo property in properties)
            //    {
            //        if (property.CanRead)
            //        {
            //            string propertyName = property.Name;
            //            object? propertyValue = property.GetValue(_consumerConfig);
            //            Console.WriteLine($" * {propertyName}: {propertyValue}");
            //        }
            //    }
            //}
        }

        public override string ToString()
        {
            string exceptionInfo = FatalException != null ? $", FatalException: {FatalException?.Message}" : string.Empty;
            return $"{nameof(KafkaPartitionAlternatingBatchConsumer<TKey, TValue>)} '{Name}': " +
                $"Running: {Running}, ShutdownOrdered: {ShutdownOrdered}, Finished: {Finished}, MaxMessagesPerBatch: {MaxMessagesPerBatch}, " +
                $"Throughput: {ProcessedMessageThroughputPerSecond:n2}/sec{exceptionInfo}";
        }
    }

    internal static class KafkaMultiTopicConsumerExtensions
    {
        // Note: Internally, this instantiates a new List to collect the items to be removed, otherwise it would
        // cause a concurrent modification exception. If you know you'll be calling this a lot, consider updating
        // this to allow the itemsToRemove list to be re-used, to reduce instantiations.
        internal static int RemoveMatches<T>(this ICollection<T> collection, Func<T, bool> predicate)
        {
            _ = collection ?? throw new ArgumentNullException(nameof(collection));
            _ = predicate ?? throw new ArgumentNullException(nameof(predicate));

            var itemsToRemove = collection.Where(predicate).ToList();
            foreach (var item in itemsToRemove)
            {
                collection.Remove(item);
            }
            return itemsToRemove.Count;
        }

        internal static bool IsAssigned<K, V>(this IConsumer<K, V> consumer, TopicPartition topicPartition)
        {
            try
            {
                return consumer.Assignment.Contains(topicPartition);
            }
            catch (Exception ex)
            {
                // When the IConsumer has been ordered to Close() and the PartitionsRevokedHandler is being called, I
                // sometimes see calls to the IConsumer's Assignment throw this error: "Local: Broker handle destroyed". I
                // don't want to throw that here. Granted, that represents a bigger problem and will therefore result in an
                // exception being thrown to a later operation on IConsumer. But I'd rather hear about that exception from a
                // more important IConsumer method.
                if (ex.Message.Contains("handle destroyed", StringComparison.OrdinalIgnoreCase))
                    return false;
                throw;
            }
        }

        internal static int CountConsecutiveSamePartitionInReverse<K, V>(this List<ConsumeResult<K, V>> messages)
        {
            ConsumeResult<K, V> lastMessage = null;
            int consecutiveCount = 0;
            // Go through the list of messages in reverse:
            for (int i = messages.Count - 1; i >= 0; i--)
            {
                var message = messages[i];
                if (lastMessage == null)
                {
                    // Then this is the last message in the list.
                    lastMessage = message;
                }
                else
                {
                    // Otherwise this is not the last message in the list. So let's see if this is in the same TopicPartition
                    // as the last message.
                    if (message.TopicPartition != lastMessage.TopicPartition)
                    {
                        // It's a different TopicPartition. Therefore, we're done counting (and we won't count this one).
                        break;
                    }
                }
                // If we're still here, count an additional message that matches
                // the last message in the list (this count includes the last message).
                consecutiveCount++;
            }
            return consecutiveCount;
        }

        /// <summary>
        /// </summary>
        /// <param name="duration">the total duration</param>
        /// <param name="count">the count of instances that occurred within that total duration</param>
        /// <returns>The average time for each instance that's being counted.</returns>
        internal static TimeSpan GetAverage(this TimeSpan duration, long count)
        {
            if (count <= 0)
                return TimeSpan.Zero;
            return duration / count;
        }

        internal static TimeSpan GetAverage(this TimeSpan duration, double count)
        {
            if (count <= 0)
                return TimeSpan.Zero;
            return duration / count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="duration">The total duration</param>
        /// <param name="count">the count of instances that occurred within that total duration</param>
        /// <returns>The number of instances that occur per second</returns>
        internal static double GetInstancesPerSecond(this TimeSpan duration, long count)
        {
            if (count < 0)
                return 0.0;
            double seconds = duration.TotalSeconds;
            if (seconds == 0.0)
                return 0.0;
            return count / seconds;
        }

        internal static bool AllInnerExceptionsAreOperationCancelled(this AggregateException aggEx)
        {
            aggEx = aggEx.Flatten();
            if (aggEx.InnerExceptions?.Count == 0)
                return false;
            return aggEx.InnerExceptions.All(IsOperationCancelled);
        }

        internal static bool IsOperationCancelled(this Exception ex)
        {
            if (ex is OperationCanceledException)
                return true;
            if (ex is AggregateException aggEx && aggEx.InnerExceptions?.Count > 0)
                return AllInnerExceptionsAreOperationCancelled(aggEx);
            return false;
        }

        public static string ToStringPretty(this TimeSpan span)
        {
            if (span.TotalMilliseconds < 100)
                return $"{span.TotalMilliseconds:n2} ms";
            else if (span.TotalMilliseconds < 3000)
                return $"{span.TotalMilliseconds:n1} ms";
            else if (span.TotalSeconds < 10)
                return $"{span.TotalSeconds:n3} sec";
            else if (span.TotalSeconds < 160)
                return $"{span.TotalSeconds:n1} sec";
            else if (span.TotalMinutes < 160)
                return $"{span.TotalMinutes:n1} min";
            else if (span.TotalHours < 48)
                return $"{span.TotalHours:n2} hours";
            else
                return $"{span.TotalDays:n2} days";
        }
    }
}
