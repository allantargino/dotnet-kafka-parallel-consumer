using Confluent.Kafka;

namespace KafkaParallelConsumer
{
    public sealed class KafkaPartitionsHandler<TKey, TValue>
    {
        private readonly ILogger<KafkaPartitionsHandler<TKey, TValue>> logger;
        private readonly ChannelProvider<TKey, TValue> channelProvider;

        public KafkaPartitionsHandler(ILogger<KafkaPartitionsHandler<TKey, TValue>> logger, ChannelProvider<TKey, TValue> channelProvider)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.channelProvider = channelProvider ?? throw new ArgumentNullException(nameof(channelProvider));
        }
        
        public void PartitionsAssignedHandler(IConsumer<TKey, TValue> _, List<TopicPartition> tps)
        {
            foreach (var tp in tps)
            {
                logger.LogInformation("TopicPartition assigned: {TopicPartition}", tp);
                channelProvider.CreateTopicPartitionChannel(tp);
            }
        }

        public void PartitionsLostHandler(IConsumer<TKey, TValue> _, List<TopicPartitionOffset> tpos)
        {
            foreach (var tpo in tpos)
            {
                logger.LogInformation("TopicPartition lost: {TopicPartition}", tpo.TopicPartition);
                channelProvider.CompleteTopicPartitionChannel(tpo.TopicPartition);
            }
        }
    }
}
