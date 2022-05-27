using Confluent.Kafka;
using System.Threading.Channels;

namespace KafkaParallelConsumer;

public sealed class ParallelWorker : BackgroundService
{
    private readonly ILogger<ParallelWorker> logger;
    private readonly IConsumer<string, string> consumer;
    private readonly ChannelProvider channelProvider;

    public ParallelWorker(ILogger<ParallelWorker> logger, IConsumer<string, string> consumer, ChannelProvider channelProvider)
    {
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        this.channelProvider = channelProvider ?? throw new ArgumentNullException(nameof(channelProvider));
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

            ChannelWriter<ConsumeResult<string, string>> channelWriter = channelProvider.GetChannelWriter(consumeResult.Topic, stoppingToken);

            await channelWriter.WriteAsync(consumeResult, stoppingToken);
        }
    }
}
