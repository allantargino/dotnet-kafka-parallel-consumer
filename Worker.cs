using Confluent.Kafka;

namespace KafkaParallelConsumer;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> logger;
    private readonly IConsumer<byte[], byte[]> consumer;

    public Worker(ILogger<Worker> logger, IConsumer<byte[], byte[]> consumer)
    {
        this.logger = logger;
        this.consumer = consumer;
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

            logger.LogInformation($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

            await Task.Delay(1000, stoppingToken);

            consumer.Commit(consumeResult);
        }
    }
}
