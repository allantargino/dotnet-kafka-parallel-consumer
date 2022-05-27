using Confluent.Kafka;

namespace KafkaParallelConsumer
{
    internal class Processor<TKey, TValue> : IProcessor<TKey, TValue>
    {
        private readonly ILogger<Processor<TKey, TValue>> logger;
        private readonly IConsumer<TKey, TValue> consumer;

        public Processor(ILogger<Processor<TKey, TValue>> logger, IConsumer<TKey, TValue> consumer)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        }

        public async Task ProcessAsync(ConsumeResult<TKey, TValue> consumeResult, CancellationToken cancellationToken)
        {
            logger.LogInformation("Processing message at {messageTopicPartitionOffset}: {messageValue}", consumeResult.TopicPartitionOffset, consumeResult.Message.Value);
            
            await Task.Delay(1000, cancellationToken);

            consumer.Commit(consumeResult);
        }
    }
}
