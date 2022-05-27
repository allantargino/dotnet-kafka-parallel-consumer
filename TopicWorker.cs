using Confluent.Kafka;
using System.Threading.Channels;

namespace KafkaParallelConsumer
{
    internal class TopicWorker
    {
        private readonly ChannelReader<ConsumeResult<string, string>> channelReader;
        private readonly Func<ConsumeResult<string, string>, CancellationToken, Task> processingAction;

        public TopicWorker(ChannelReader<ConsumeResult<string, string>> channelReader,
            Func<ConsumeResult<string, string>, CancellationToken, Task> processingAction)
        {
            this.channelReader = channelReader ?? throw new ArgumentNullException(nameof(channelReader));
            this.processingAction = processingAction ?? throw new ArgumentNullException(nameof(processingAction));
        }

        internal async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            while (await channelReader.WaitToReadAsync(cancellationToken))
            {
                while (channelReader.TryRead(out var item))
                {
                    await processingAction(item, cancellationToken);
                }
            }
        }
    }
}
