using Confluent.Kafka;
using System.Threading.Channels;

namespace KafkaParallelConsumer
{
    public sealed class TopicPartitionWorker<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> consumer;
        private readonly ChannelReader<ConsumeResult<TKey, TValue>> channelReader;
        private readonly Func<ConsumeResult<TKey, TValue>, CancellationToken, Task> processingAction;
        private readonly bool commitSynchronous;
        private readonly int commitPeriod;

        public TopicPartitionWorker(IConsumer<TKey, TValue> consumer, ChannelReader<ConsumeResult<TKey, TValue>> channelReader,
            Func<ConsumeResult<TKey, TValue>, CancellationToken, Task> processingAction, bool commitSynchronous, int commitPeriod = 5)
        {
            this.consumer = consumer;
            this.channelReader = channelReader ?? throw new ArgumentNullException(nameof(channelReader));
            this.processingAction = processingAction ?? throw new ArgumentNullException(nameof(processingAction));
            this.commitSynchronous = commitSynchronous;
            this.commitPeriod = commitPeriod;
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            while (await channelReader.WaitToReadAsync(cancellationToken))
            {
                while (channelReader.TryRead(out ConsumeResult<TKey, TValue>? consumeResult))
                {
                    await processingAction(consumeResult, cancellationToken);

                    if (commitSynchronous || consumeResult.Offset % commitPeriod == 0)
                    {
                        consumer.Commit(consumeResult);
                    }
                }
            }
        }
    }
}
