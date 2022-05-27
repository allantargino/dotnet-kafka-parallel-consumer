using Confluent.Kafka;
using KafkaParallelConsumer;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(c => c.AddSimpleConsole(o =>
    {
        o.SingleLine = true;
        o.ColorBehavior = Microsoft.Extensions.Logging.Console.LoggerColorBehavior.Enabled;
        o.IncludeScopes = true;
        o.TimestampFormat = "hh:mm:ss:fff ";
    }))
    .ConfigureServices((hostContext, services) =>
  {
      var config = new ConsumerConfig
      {
          BootstrapServers = "localhost:9092",
          GroupId = "csharp-consumer2",
          EnableAutoCommit = false,
          StatisticsIntervalMs = 5000,
          SessionTimeoutMs = 6000,
          AutoOffsetReset = AutoOffsetReset.Earliest,
          EnablePartitionEof = false,
          // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
          // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
          PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
      };

      if (hostContext.Configuration.GetValue<bool>("UseParallel"))
      {
          services.AddHostedService<ParallelWorker>();
          services.AddSingleton<ChannelProvider>();
          services.AddSingleton(_ => new ConsumerBuilder<string, string>(config)
                                            //.SetPartitionsAssignedHandler(PartitionsAssignedHandler) //TODO
                                            .Build());
      }
      else
      {
          services.AddHostedService<SerialWorker>();
          services.AddSingleton(_ => new ConsumerBuilder<string, string>(config)
                                            .Build());
      }

      services.AddSingleton<IProcessor<string, string>, Processor<string, string>>();
  })
    .Build();

await host.RunAsync();
