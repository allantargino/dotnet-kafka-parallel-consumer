using Confluent.Kafka;

namespace KafkaParallelConsumer
{
    public interface IProcessor<TValue, TKey>
    {
        Task ProcessAsync(ConsumeResult<TValue, TKey> consumeResult, CancellationToken cancellationToken);
    }
}