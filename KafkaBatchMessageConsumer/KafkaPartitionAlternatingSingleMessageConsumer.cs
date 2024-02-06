using Confluent.Kafka;
using System;
using System.Collections.Immutable;

namespace KafkaBatchMessageConsumer
{
    public abstract class KafkaPartitionAlternatingSingleMessageConsumer<K, V, TModel>
        : KafkaPartitionAlternatingBatchConsumer<K, V>, IKafkaPartitionAlternatingConsumer
        where TModel : class
    {
        public const int DEFAULT_BATCH_SIZE = 50;

        public KafkaPartitionAlternatingSingleMessageConsumer(string name, ConsumerConfig config,
            int maxMessagesPerBatch = DEFAULT_BATCH_SIZE)
            : base(name, config, maxMessagesPerBatch)
        {
            // If an entire batch is from one Partition, then we want the parent's
            // main thread to Pause & Resume that Partition while our thread is processing,
            // so that the next batch comes from a different Partition.
            MaxConsecutiveMessagesPerPartition = MaxMessagesPerBatch;
        }

        /// <summary>
        /// <para>
        /// Prior to a batch of messages being processed, this is called to allow each message (of type ConsumeResult)
        /// to be transformed into type TModel. This returns a List of TModel, where each model corresponds to the
        /// element at the same index in the passed "messages" list.
        /// </para>
        /// <para>
        /// This also has the opportunity to filter-out messages that it doesn't want to process.
        /// This can put a null TModel object into an element in the returned List to indicate that this message
        /// should be skipped: It won't be sent to PeekAtNextBatchAsync() or ProcessMessageAsync(), instead it
        /// will be Ack'd so that the message goes away and is not re-delivered. If you're processor doesn't want
        /// to process every type of Event message, then this is a good way to skip the ones you don't want.
        /// </para>
        /// <para>
        /// Note: This method is not async. You shouldn not perform side-effect tasks here, such as calling a database.
        /// This is meant to be quick. Note that the subsequent method, PeekAtNextBatchAsync(), is async.
        /// </para>
        /// <para>
        /// There's another option that you can also take advantage of (but this will likely be more rare):
        /// If you want to Nack a message, then simply return a shorter List. For example, if 10 messages are passed
        /// to TransformAndFilter(), and this returns a List with Count == 5, then those 5 will be processed and Ack'd
        /// (or if they're null they'll be skipped & Ack'd). But since the List has Count == 5, that 5th message will
        /// be the last one that's Ack'd, and every subsequent message will be Nack'd. Remember: Messages must be
        /// Nack'd and Ack'd in order, so if you choose to Nack a message then every message after it must also be Nack'd.
        /// </para>
        /// </summary>
        /// <param name="messages"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        protected abstract List<TModel?> TransformAndFilter(IReadOnlyList<ConsumeResult<K, V>> messages, CancellationToken cancelToken);

        /// <summary>
        /// When the super class loads a new batch of messages, prior to passing any of those individual messages
        /// to your ProcessMessagesAsync() method, it will pass all of them to this method. It calls this method
        /// once per batch. The passed messages have already been transformed and filtered by the TransformAndFilter()
        /// method. This gives you the opportunity to do any bulk pre-loading of data prior to processing each
        /// individual message. Although it might seem like you could also do this in the TransformAndFilter() method,
        /// that method is not async, so it's not suitable for side-effects like making calls to a database.
        /// </summary>
        /// <param name="modelsAndMessages"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        protected virtual Task PeekAtNextBatch(ImmutableList<(TModel Model, ConsumeResult<K, V> Message)> modelsAndMessages, CancellationToken cancelToken)
        {
            return Task.CompletedTask;
        }

        protected abstract Task<bool> ProcessMessageAsync((TModel Model, ConsumeResult<K, V> Message) modelAndMessage, CancellationToken cancelToken);

        protected override sealed async Task ProcessBatchAsync(IReadOnlyList<ConsumeResult<K, V>> messages, IAcknowledgement acknowledgement, CancellationToken cancelToken)
        {
            // We give the sub-class the opportunity to transform these messages from ConsumeResult<K,V> into type TModel.
            // This method also has the ability to filter messages, in ways described below.
            // It's expexted to return a List, which should generally have the same number of elements in it as passed.
            // Each element in the returned List is the transformed model that corresponds to the ConsumeResult at the
            // same index as the passed "messages" ReadOnlyList.
            // Any of the elements can be null, which tells us that we should skip that message. In that case, we Ack that
            // message in the Event Broker (Kafka), so that it gets skipped and not repeated in the future.
            // There's another option: If this Transform method implementation decides it wants to Nack messages, then
            // it should return a shorter List. So if 10 messages are passed to TransformAndFilter(), and that returns a
            // List with Count == 5, then those 5 will be processed and Ack'd (or if they're null they'll be skipped & Ack'd).
            // But since the List has Count == 5, that 5th message will be the last one that's Ack'd, and every
            // ConsumeResult in "messages" after the 5th will be Nack'd.
            List<TModel?> transformedMessages = TransformAndFilter(messages, cancelToken);

            _ = transformedMessages ?? throw new NullReferenceException($"{nameof(KafkaPartitionAlternatingSingleMessageConsumer<K, V, TModel>)}." +
                $"{nameof(ProcessBatchAsync)}: {nameof(TransformAndFilter)}() returned null.");
            if (transformedMessages.Count == 0)
            {
                // If Transform() returned an empty List then it wants all messages to be Nack'd. So don't bother to call
                // PeekAtNextBatch(). So return now without doing anything. All un-Ack'd messages will be Nack'd by the caller.
                return;
            }

            // Zip together a new list that contains each transformed Model corresponding to its ConsumeResult (the "Message").
            // Note that this list is only as long as the transformedMessages List. If this list is shorter than the passed
            // "messages" List, this means that any messages not included here will be Nack'd.
            ImmutableList<(TModel? Model, ConsumeResult<K, V> Message)> modelsAndMessages = transformedMessages.Zip(messages).ToImmutableList();

            ImmutableList<(TModel Model, ConsumeResult<K, V> Message)> modelsAndMessagesWithoutNulls;
            if (modelsAndMessages.Any(mm => mm.Model == null))
            {
                modelsAndMessagesWithoutNulls = modelsAndMessages.Where(mm => mm.Model != null).ToImmutableList();
            }
            else
            {
                // Since there are no null models, don't bother insantiating a new ImmutableList which contains
                // the same elements as the modelsAndMessages ImmutableList. Just share the latter reference:
                modelsAndMessagesWithoutNulls = modelsAndMessages;
            }

            // Let the sub-class "peek" at the batch of models/messages that it's about to process. We omit any that are null
            // in the transformedMessages List, as well as any that don't have a corresponding element in transformedMessages.
            await PeekAtNextBatch(modelsAndMessagesWithoutNulls, cancelToken);

            // Loop through every Model & ConsumeResult, including ones where Model is null.
            // Note: If modelsAndMessages is shorter than the passed "messages" List, that's fine, just means that those
            // messages that aren't in modelsAndMessages will be Nack'd (by simply not Ack'ing them).
            for (int i = 0; i < modelsAndMessages.Count; i++)
            {
                if (cancelToken.IsCancellationRequested)
                {
                    break;
                }

                var modelAndConsumeResult = modelsAndMessages[i];

                ThrowIfTopicPartitionNotAssignedToThisConsumer(modelAndConsumeResult.Message.TopicPartition);

                // Pass this to ProcessMessageAsync() for processing. If that returns true then we Ack this message.
                // Otherwise, we Nack this message along with every subsequent message in this batch.
                // Caveat: If the Model is null, that means we should skip processing of this message and Ack it.
                // This can happen because the Transform() message is allowed to act as a "filter" by providing
                // null Models for messages that it wants to skip (remember, "skip" means to Ack it without processing).
                if (modelAndConsumeResult.Model == null || await ProcessMessageAsync(modelAndConsumeResult, cancelToken).ConfigureAwait(false))
                {
                    acknowledgement.Ack(i + 1);
                }
                else
                {
                    // If ProcessMessageAsync returns false, that means that we Nack that message and all subsequent
                    // messages. So we simply end here, because all un-Ack'd messages will be Nack'd by the caller.
                    break;
                }
            }
        }
    }
}
