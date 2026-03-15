using System.Text;
using System.Text.Json;
using Notificacoes.Api.Models;
using RabbitMQ.Client;

namespace Notificacoes.Api.Messaging;

public class NotificationProducer(IConnection connection)
{
    private const string QueueName = "email_notifications_queue";
    private const string RoutingKeyEmail = "notification.email";

    private const string DlxName = "email_notifications_dlx";
    private const string ExchangeNotification = "notifications_exchange";
    private const string RoutingKeyDLQ = "error.email";

    public async Task ProduceAsync(NotificationMessage message)
    {
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);

        await using var channel = await connection.CreateChannelAsync(options);

        var queueArgs = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", DlxName },
            { "x-dead-letter-routing-key", RoutingKeyDLQ }
        };

        await channel.ExchangeDeclareAsync(
            exchange: ExchangeNotification,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false         
        );

        await channel.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs
        );

        await channel.QueueBindAsync(
            queue: QueueName,
            exchange: ExchangeNotification,
            routingKey: RoutingKeyEmail
        );

        var json = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(json);

        var properties = new BasicProperties
        {
            DeliveryMode = DeliveryModes.Persistent,
            ContentType = "application/json"
        };

        await channel.BasicPublishAsync(
            exchange: ExchangeNotification,
            routingKey: "notification.email",
            body: body,
            mandatory: true,
            basicProperties: properties
        );
    }
}
