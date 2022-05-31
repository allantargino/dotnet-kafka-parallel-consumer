using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace KafkaParallelConsumer;

public sealed class Worker<TKey, TValue> : BackgroundService
{
    private readonly ILogger<Worker<TKey, TValue>> logger;
    private readonly IConsumer<TKey, TValue> consumer;
    private readonly Processor<TKey, TValue> processor;
    private readonly ChannelProvider<TKey, TValue> channelProvider;
    private readonly IEnumerable<string> topics;

    public Worker(ILogger<Worker<TKey, TValue>> logger, IConsumer<TKey, TValue> consumer,
        Processor<TKey, TValue> processor, ChannelProvider<TKey, TValue> channelProvider, IOptions<WorkerOptions> options)
    {
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        this.processor = processor ?? throw new ArgumentNullException(nameof(processor));
        this.channelProvider = channelProvider ?? throw new ArgumentNullException(nameof(channelProvider));

        if (options?.Value?.Topics is null) throw new ArgumentNullException(nameof(options));
        topics = options.Value.Topics;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // because consumer.Consume() blocks, Task.Yield() fixes https://github.com/dotnet/runtime/issues/36063 without creating a new thread or using Task.Run
        await Task.Yield();

        logger.LogInformation("Subscribing to {topics}", string.Join(',', topics));
        consumer.Subscribe(topics);

        while (!stoppingToken.IsCancellationRequested)
        {
            var consumeResult = consumer.Consume(stoppingToken);

            logger.LogTrace("Received message at {messageTopicPartitionOffset}: {messageValue}", consumeResult.TopicPartitionOffset, consumeResult.Message.Value);

            var channelWriter = channelProvider.GetChannelWriter(consumer, consumeResult.TopicPartition, processor.ProcessAsync, stoppingToken);

            await channelWriter.WriteAsync(consumeResult, stoppingToken);
        }
    }
}
