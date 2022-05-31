using Confluent.Kafka;

namespace KafkaParallelConsumer
{
    public sealed class Processor<TKey, TValue>
    {
        private readonly ILogger<Processor<TKey, TValue>> logger;

        public Processor(ILogger<Processor<TKey, TValue>> logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task ProcessAsync(ConsumeResult<TKey, TValue> consumeResult, CancellationToken cancellationToken)
        {
            logger.LogTrace("Processing message at {messageTopicPartitionOffset}: {messageValue}", consumeResult.TopicPartitionOffset, consumeResult.Message.Value);
            
            // Simulates some async processing
            await Task.Delay(1000, cancellationToken);
        }
    }
}
