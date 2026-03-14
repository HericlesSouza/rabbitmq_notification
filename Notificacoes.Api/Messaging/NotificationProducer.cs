using System.Text;
using System.Text.Json;
using Notificacoes.Api.Models;
using RabbitMQ.Client;

namespace Notificacoes.Api.Messaging;

public class NotificationProducer(IConnection connection)
{
    private const string QueueName = "email_notifications_queue";

    private const string DlxName = "email_notifications_dlx";
    private const string RoutingKey = "error.email";

    public async Task ProduceAsync(NotificationMessage message)
    {
        await using var channel = await connection.CreateChannelAsync();

        var queueArgs = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", DlxName },
            { "x-dead-letter-routing-key", RoutingKey }
        };

        await channel.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs           
        );

        var json = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(json);

        var properties = new BasicProperties
        {
            DeliveryMode = DeliveryModes.Persistent,
            ContentType = "application/json"
        };

        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: QueueName,
            body: body,
            mandatory: true,
            basicProperties: properties
        );
    }
}
