namespace ConsumerAPICSharp;
using Confluent.Kafka;
using System;


class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("Hello, World! Consumer");

        var config = new ConsumerConfig {
            BootstrapServers = "localhost:9092",
            GroupId = "kafka-dotnet-getting-started"
            };

        using (var consumer = new ConsumerBuilder<string, string>(config).Build())
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            consumer.Subscribe("ClassAtt");
            try {
                while (true) {
                    var cr = consumer.Consume(cts.Token);
                    Console.Write(cr.Message.Key);
                    Console.Write(" --- " + cr.Message.Value);
                    Console.WriteLine();
                }
            }
            catch (OperationCanceledException) {
                // Ctrl-C was pressed.
            }
            finally{
                consumer.Close();
            }
        }

        Console.Read();
    }
}
