using Confluent.Kafka;
using System.Diagnostics;

namespace KafkaBatchMessageConsumer
{
    [DebuggerDisplay("{DebuggerDisplay,nq}")]
    public class TopicPartitionWatermarkInfo
    {
        public TopicPartition TopicPartition { get; }
        public DateTimeOffset GeneratedAt { get; }
        public Offset Current { get; }
        public WatermarkOffsets Watermarks { get; }
        public long OffsetLag { get; } = Offset.Unset;
        public TimeSpan EstimatedLagTime { get; } = TimeSpan.Zero;

        internal TopicPartitionWatermarkInfo(TopicPartition topicPartition, WatermarkOffsets watermarkOffsets, Offset currentOffset, TimeSpan averageProcessingTimePerMessage)
        {
            TopicPartition = topicPartition ?? throw new ArgumentNullException(nameof(topicPartition));

            GeneratedAt = DateTimeOffset.Now;

            Current = currentOffset;
            Watermarks = watermarkOffsets;

            // We can only generate these if we have valid Offsets:
            if (!currentOffset.IsSpecial && watermarkOffsets.High != Offset.Unset)
            {
                // How far "behind" (in terms of Offset) is the currentOffset? If currentOffset is the highest Offset then this should be zero.
                // Note: WatermarkOffsets.High is the highest Offset in the partition PLUS 1, which is why we subtract one here.
                OffsetLag = !currentOffset.IsSpecial ? Math.Max((watermarkOffsets.High - 1) - currentOffset.Value, 0) : -1;

                EstimatedLagTime = averageProcessingTimePerMessage * OffsetLag;
            }
        }

        public override string ToString()
        {
            return $"TopicPartition: {TopicPartition}, current offset: {Current}, " +
                $"high watermark: {Watermarks?.High.ToString() ?? "n/a"}, offset lag: {OffsetLag}, lag time: {EstimatedLagTime.ToStringPretty()}, " +
                $"low watermark: {Watermarks?.Low.ToString() ?? "n/a"}, generated at: {GeneratedAt}";
        }

        private string DebuggerDisplay => $"{nameof(TopicPartitionWatermarkInfo)}: {ToString()}";
    }
}
