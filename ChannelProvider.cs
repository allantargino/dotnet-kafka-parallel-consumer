using Confluent.Kafka;
using System.Threading.Channels;

namespace KafkaParallelConsumer;

public sealed class ChannelProvider
{
    private readonly Dictionary<string, Channel<ConsumeResult<string, string>>> channels;
    private readonly List<Task> workers;
    private readonly IProcessor<string, string> processor;

    public ChannelProvider(IProcessor<string, string> processor)
    {
        this.processor = processor ?? throw new ArgumentNullException(nameof(processor));
        this.channels = new Dictionary<string, Channel<ConsumeResult<string, string>>>();
        this.workers = new List<Task>();
    }

    public IEnumerable<TopicPartitionOffset> PartitionsAssignedHandler(IConsumer<string, string> _, List<TopicPartition> topicPartitions)
    {
        var tpo = new TopicPartitionOffset[topicPartitions.Count];

        for (int i = 0; i < topicPartitions.Count; i++)
            tpo[i] = new TopicPartitionOffset(topicPartitions[i], Offset.Unset);

        return tpo;
    }

    public ChannelWriter<ConsumeResult<string, string>> GetChannelWriter(string topic, CancellationToken cancellationToken)
    {
        if (!channels.TryGetValue(topic, out var channel))
        {
            channel = Channel.CreateUnbounded<ConsumeResult<string, string>>(new UnboundedChannelOptions()
            {
                AllowSynchronousContinuations = false,
                SingleReader = true,
                SingleWriter = true,
            });

            channels.Add(topic, channel);

            var topicWorker = new TopicWorker(channel.Reader, processor);

            workers.Add(topicWorker.ExecuteAsync(cancellationToken));
        }

        return channel.Writer;
    }
}
