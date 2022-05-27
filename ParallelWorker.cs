using Confluent.Kafka;
using System.Threading.Channels;

namespace KafkaParallelConsumer;

public class ParallelWorker : BackgroundService
{
    private readonly ILogger<ParallelWorker> logger;
    private readonly IConsumer<string, string> consumer;
    private readonly IProcessor<string, string> processor;

    private readonly Dictionary<string, ChannelWriter<ConsumeResult<string, string>>> channels;
    private readonly List<Task> workers;

    public ParallelWorker(ILogger<ParallelWorker> logger, IConsumer<string, string> consumer, IProcessor<string, string> processor)
    {
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        this.processor = processor ?? throw new ArgumentNullException(nameof(processor));

        this.channels = new Dictionary<string, ChannelWriter<ConsumeResult<string, string>>>();
        this.workers = new List<Task>();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        string[] topics = new[] { "dev.input1", "dev.input2" };
        logger.LogInformation("Subscription to {topics}", string.Join(',', topics));
        consumer.Subscribe(topics);

        while (!stoppingToken.IsCancellationRequested)
        {
            var consumeResult = consumer.Consume(stoppingToken);

            logger.LogInformation("Received message at {messageTopicPartitionOffset}: {messageValue}", consumeResult.TopicPartitionOffset, consumeResult.Message.Value);

            ChannelWriter<ConsumeResult<string, string>> channelWriter;
            if (!channels.TryGetValue(consumeResult.Topic, out channelWriter!))
            {
                var channel = Channel.CreateUnbounded<ConsumeResult<string, string>>(new UnboundedChannelOptions()
                {
                    AllowSynchronousContinuations = false,
                    SingleReader = true,
                    SingleWriter = true,
                });

                channels.Add(consumeResult.Topic, channel.Writer);

                var topicWorker = new TopicWorker(channel.Reader, processor);

                workers.Add(topicWorker.ExecuteAsync(stoppingToken));

                channelWriter = channel.Writer;
            }

            await channelWriter.WriteAsync(consumeResult, stoppingToken);
        }
    }
}
