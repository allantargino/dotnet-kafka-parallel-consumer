using Confluent.Kafka;
using System.Threading.Channels;

namespace KafkaParallelConsumer;

public sealed class ChannelProvider<TKey, TValue>
{
    private readonly Dictionary<TopicPartition, Channel<ConsumeResult<TKey, TValue>>> channels;
    private readonly Dictionary<TopicPartition, Task> workers;

    public ChannelProvider()
    {
        this.channels = new Dictionary<TopicPartition, Channel<ConsumeResult<TKey, TValue>>>();
        this.workers = new Dictionary<TopicPartition, Task>();
    }

    public ChannelWriter<ConsumeResult<TKey, TValue>> GetChannelWriter(IConsumer<TKey, TValue> consumer, TopicPartition topicPartition,
        Func<ConsumeResult<TKey, TValue>, CancellationToken, Task> processingAction, CancellationToken cancellationToken)
    {
        var channel = channels[topicPartition];

        if (!workers.ContainsKey(topicPartition))
        {
            var topicWorker = new TopicPartitionWorker<TKey, TValue>(consumer, channel.Reader, processingAction, commitSynchronous: false);

            workers.Add(topicPartition, topicWorker.ExecuteAsync(cancellationToken));
        }

        return channel.Writer;
    }

    internal void CreateTopicPartitionChannel(TopicPartition topicPartition)
    {
        if (!channels.ContainsKey(topicPartition))
        {
            var channel = Channel.CreateUnbounded<ConsumeResult<TKey, TValue>>(new UnboundedChannelOptions()
            {
                AllowSynchronousContinuations = false,
                SingleReader = true,
                SingleWriter = true,
            });

            channels.Add(topicPartition, channel);
        }
    }

    public void CompleteTopicPartitionChannel(TopicPartition topicPartition)
    {
        var channel = channels[topicPartition];
        channel.Writer.Complete();
    }
}
