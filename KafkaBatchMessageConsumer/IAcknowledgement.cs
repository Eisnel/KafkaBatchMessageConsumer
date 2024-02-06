namespace KafkaBatchMessageConsumer
{
    public interface IAcknowledgement
    {
        /// <summary>
        /// <para>
        /// Pass the count of messages in the current batch's List which should be Ack'd/Committed.
        /// Shortly after this is called, a background process will Commit Offsets for the
        /// first "count" messages in the batch. If you never call this (or only pass zero)
        /// then none of the batch's messages will be Ack'd, so they'll all be Nack'd, which
        /// means that they'll be re-delivered in a future batch. You can call this as frequently
        /// as you'd like. If you process messages one at a time, then you'd likely want to call
        /// this after each message (passing a higher count each time). If you process all of the
        /// messages at the same time then you can call this at the end with the full count.
        /// Or you can process sub-batches and update this after each batch accordingly.
        /// </para>
        /// <para>
        /// Note: Since the parameter is a "count", it is one-based, not zero-based.
        /// So after you process the first message (which is at index 0), you'd call Ack(1),
        /// NOT Ack(0). You're telling Ack() that the first n messages can be Ack'd.
        /// </para>
        /// <para>
        /// Important: Maintain the order of the messages in the batch. You cannot Ack/Commit
        /// messages out-of-order. If you pass count = 3 then you'll Ack the first 3 messages.
        /// There's no way to Ack the third message without also Ack'ing the second and the first.
        /// </para>
        /// <para>
        /// Warning: You cannot un-Ack something that you've already Ack'd. If you call this
        /// with count = 3, then during that same batch you can't call this and pass a number
        /// less than 3.</para>
        /// <para>
        /// Note: This method returns immediately. No side-effect occurs during this method call.
        /// The Ack'd messages will be Committed in the background.
        /// </para>
        /// </summary>
        /// <param name="count">The number of messages (in the corresponding List of messages)
        /// that should be Ack'd.</param>
        /// <exception cref="ArgumentOutOfRangeException">This will be thrown if you pass a count
        /// greater than the number of messages in the batch, if you pass a count less than a
        /// prior count, or if you pass a negative number.</exception>
        public void Ack(int count);
    }
}
