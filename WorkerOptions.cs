namespace KafkaParallelConsumer
{
    public sealed class WorkerOptions
    {
        public IEnumerable<string> Topics { get; set; } = default!;
    }
}
