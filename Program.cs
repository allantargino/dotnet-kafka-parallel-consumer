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
        var configuration = hostContext.Configuration.GetRequiredSection("Kafka:Consumer");

        services.AddSingleton(svcProvider =>
        {
            var channelProvider = svcProvider.GetRequiredService<ChannelProvider<string, string>>();
            var consumerConfig = configuration.GetRequiredSection("Client").Get<ConsumerConfig>();

            return new ConsumerBuilder<string, string>(consumerConfig)
                                          .SetPartitionsAssignedHandler(channelProvider.PartitionsAssignedHandler)
                                          .SetPartitionsLostHandler(channelProvider.PartitionsLostHandler)
                                          .Build();
        });
        services.AddSingleton<Processor<string, string>>();
        services.AddSingleton<ChannelProvider<string, string>>();
        services.AddHostedService<Worker<string, string>>()
                .Configure<WorkerOptions>(configuration);
    })
    .Build();

await host.RunAsync();
