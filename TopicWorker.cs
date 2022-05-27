using Confluent.Kafka;
using System.Threading.Channels;

namespace KafkaParallelConsumer
{
    internal class TopicWorker
    {
        private readonly ChannelReader<ConsumeResult<string, string>> channelReader;
        private readonly IProcessor<string, string> processor;

        public TopicWorker(ChannelReader<ConsumeResult<string, string>> channelReader, IProcessor<string, string> processor)
        {
            this.channelReader = channelReader;
            this.processor = processor;
        }

        internal async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            ConsumeResult<string, string> item;
            while (await channelReader.WaitToReadAsync(cancellationToken))
            {
                while (channelReader.TryRead(out item!))
                {
                    await processor.ProcessAsync(item, cancellationToken);
                }
            }
        }
    }
}
