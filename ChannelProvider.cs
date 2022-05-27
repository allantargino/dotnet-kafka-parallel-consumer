using Confluent.Kafka;
using System.Threading.Channels;

namespace KafkaParallelConsumer;

public sealed class ChannelProvider
{
    private readonly Dictionary<TopicPartition, Channel<ConsumeResult<string, string>>> channels;
    private readonly Dictionary<TopicPartition, Task> workers;
    private readonly ILogger<ChannelProvider> logger;

    public ChannelProvider(ILogger<ChannelProvider> logger)
    {
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.channels = new Dictionary<TopicPartition, Channel<ConsumeResult<string, string>>>();
        this.workers = new Dictionary<TopicPartition, Task>();
    }

    public ChannelWriter<ConsumeResult<string, string>> GetChannelWriter(TopicPartition topicPartition, Func<ConsumeResult<string, string>, CancellationToken, Task> processingAction, CancellationToken cancellationToken)
    {
        var channel = channels[topicPartition];

        if (!workers.ContainsKey(topicPartition))
        {
            var topicWorker = new TopicWorker(channel.Reader, processingAction);

            workers.Add(topicPartition, topicWorker.ExecuteAsync(cancellationToken));
        }

        return channel.Writer;
    }

    public IEnumerable<TopicPartitionOffset> PartitionsAssignedHandler(IConsumer<string, string> _, List<TopicPartition> tps)
    {
        var tpos = new TopicPartitionOffset[tps.Count];
        for (int i = 0; i < tps.Count; i++)
        {
            logger.LogInformation("TopicPartition assigned: {TopicPartition}", tps[i]);

            tpos[i] = new TopicPartitionOffset(tps[i], Offset.Unset);

            if (!channels.ContainsKey(tps[i]))
            {
                var channel = Channel.CreateUnbounded<ConsumeResult<string, string>>(new UnboundedChannelOptions()
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

    public IEnumerable<TopicPartitionOffset> PartitionsLostHandler(IConsumer<string, string> _, List<TopicPartitionOffset> tpos)
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
