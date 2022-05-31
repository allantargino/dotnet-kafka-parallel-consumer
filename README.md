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

I really like VSCode [jeppeandersen.vscode-kafka](https://marketplace.visualstudio.com/items?itemName=jeppeandersen.vscode-kafka) extension to manually interact with Kafka. The file `producer.kafka` contains some code that the extension uses to generate sample messages.

![image](https://user-images.githubusercontent.com/13934447/171197286-517be335-4f5a-4e5e-bfe2-e4e70c644d26.png)
