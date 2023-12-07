namespace ConsumerAPICSharp;
using Confluent.Kafka;
using System;


class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("Hello, World! Consumer");

        var config = new ConsumerConfig {
            BootstrapServers = "localhost:9092"
            ,GroupId = "grp1"
            ,AutoOffsetReset = AutoOffsetReset.Earliest
            };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            consumer.Subscribe("ClassAtt");
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cts.Cancel();
            };

            try {
                // var incr = 1;
                // consumer.ConsumeFromLatest("ClassAtt");
                // consumer.MessageReceived += record => 
                // {
                //      Console.Write(incr.ToString() + " - " + Encoding.UTF8.GetString(record.Value));
                //      Console.WriteLine();
                //      incr++;
                // };

                while (true) {
                    var cr = consumer.Consume(cts.Token);
                    Console.WriteLine(" --- " + cr.Message.Value);
                    Console.WriteLine();
                }
            }
            catch (ConsumeException ex) {
                Console.WriteLine($"Error Occured: {ex.Message}");
                // Ctrl-C was pressed.
            }
            finally{
                consumer.Close();
            }
        }
    }
}
