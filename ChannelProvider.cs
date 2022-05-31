using Confluent.Kafka;
using System.Threading.Channels;

namespace KafkaParallelConsumer;

public sealed class ChannelProvider<TKey, TValue>
{
    private readonly Dictionary<TopicPartition, Channel<ConsumeResult<TKey, TValue>>> channels;
    private readonly Dictionary<TopicPartition, Task> workers;
    private readonly ILogger<ChannelProvider<TKey, TValue>> logger;

    public ChannelProvider(ILogger<ChannelProvider<TKey, TValue>> logger)
    {
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

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

    public IEnumerable<TopicPartitionOffset> PartitionsAssignedHandler(IConsumer<TKey, TValue> _, List<TopicPartition> tps)
    {
        var tpos = new TopicPartitionOffset[tps.Count];
        for (int i = 0; i < tps.Count; i++)
        {
            logger.LogInformation("TopicPartition assigned: {TopicPartition}", tps[i]);

            tpos[i] = new TopicPartitionOffset(tps[i], Offset.Unset);

            if (!channels.ContainsKey(tps[i]))
            {
                var channel = Channel.CreateUnbounded<ConsumeResult<TKey, TValue>>(new UnboundedChannelOptions()
                {
                    AllowSynchronousContinuations = false,
                    SingleReader = true,
                    SingleWriter = true,
                });

                channels.Add(tps[i], channel);
            }
        }
        return tpos;
    }

    public IEnumerable<TopicPartitionOffset> PartitionsLostHandler(IConsumer<TKey, TValue> _, List<TopicPartitionOffset> tpos)
    {
        foreach (var tpo in tpos)
        {
            logger.LogInformation("TopicPartition lost: {TopicPartition}", tpo.TopicPartition);

            var channel = channels[tpo.TopicPartition];
            channel.Writer.Complete();
        }

        return tpos;
    }
}
