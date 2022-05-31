# dotnet-kafka-parallel-consumer

This sample uses [.NET Channels](https://devblogs.microsoft.com/dotnet/an-introduction-to-system-threading-channels/) to create in-memory queues to support
one worker per Kafka topic partition.

## Requirements

- .NET 6
- Docker

## Quickstart

Start Kafka:

```
docker-compose up
```

Run the Worker:
```
dotnet run
```

## Producing sample messages

I really like VSCode [jeppeandersen.vscode-kafka](https://marketplace.visualstudio.com/items?itemName=jeppeandersen.vscode-kafka) extension.
The file `producer.kafka` contains some code that the extension uses to generate sample messages.
