namespace KafkaBatchMessageConsumer
{
    public interface IKafkaPartitionAlternatingConsumer
    {
        string Name { get; }
        int MaxConsecutiveMessagesPerPartition { get; }
        int ShutdownGracePeriodInMilliseconds { get; set; }
        Exception? FatalException { get; }
        bool Running { get; }
        bool ShutdownOrdered { get; }
        bool Finished { get; }
        CancellationToken FinishedToken { get; }
        TimeSpan Runtime { get; }
        TimeSpan NonIdleTime { get; }
        long AckedMessageCount { get; }
        double NonIdleThroughputPerSecond { get; }
        double ProcessedMessageThroughputPerSecond { get; }
        TimeSpan AverageMessageProcessingTime { get; }
        TimeSpan AverageBatchProcessingTime { get; }
        public IEnumerable<TopicPartitionWatermarkInfo> WatermarkStatistics { get; }

        void RunSynchronously(IEnumerable<string> topics, CancellationToken externalCancelToken);
        public CancellationToken Shutdown();
        string GetStats();
        void WriteStats(TextWriter writer);
    }

    public interface IKafkaPartitionAlternatingBatchConsumer : IKafkaPartitionAlternatingConsumer
    {
        int MaxMessagesPerBatch { get; }
    }
}
