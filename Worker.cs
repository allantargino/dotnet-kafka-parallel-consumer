using Confluent.Kafka;

namespace KafkaParallelConsumer;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> logger;
    private readonly IConsumer<string, string> consumer;
    private readonly IProcessor<string, string> processor;

    public Worker(ILogger<Worker> logger, IConsumer<string, string> consumer, IProcessor<string, string> processor)
    {
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        this.processor = processor ?? throw new ArgumentNullException(nameof(processor));
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

            await processor.ProcessAsync(consumeResult, stoppingToken);
        }
    }
}
