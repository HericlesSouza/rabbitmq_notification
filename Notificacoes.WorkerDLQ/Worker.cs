using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Notificacoes.WorkerDLQ;

public class Worker(IConnection connection, ILogger<Worker> logger) : BackgroundService
{
    private const string DlxName = "email_notifications_dlx";
    private const string DlqName = "email_notifications_dlq";
    private const string RoutingKey = "error.email";
    private CancellationToken _stoppingToken = default;
    private AsyncEventingBasicConsumer? _consumer;
    private IChannel? _channel;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;

        _channel = await connection.CreateChannelAsync(cancellationToken: _stoppingToken);

        await _channel.BasicQosAsync(
            prefetchCount: 10,
            prefetchSize: 0,
            global: false,
            cancellationToken: _stoppingToken
        );

        await _channel.ExchangeDeclareAsync(
            exchange: DlxName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: _stoppingToken
        );

        await _channel.QueueDeclareAsync(
            queue: DlqName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: _stoppingToken
        );

        await _channel.QueueBindAsync(
            queue: DlqName,
            exchange: DlxName,
            routingKey: RoutingKey,
            cancellationToken: _stoppingToken
        );

        _consumer = new AsyncEventingBasicConsumer(_channel);

        _consumer.ReceivedAsync += OnMessageReceived;

        await _channel.BasicConsumeAsync(
            queue: DlqName,
            autoAck: false,
            consumer: _consumer,
            cancellationToken: _stoppingToken);
    }

    private async Task OnMessageReceived(object sender, BasicDeliverEventArgs ea)
    {
        try
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            logger.LogInformation("Nova mensagem na DLQ");

            await Task.Delay(1000, _stoppingToken);

            logger.LogInformation("Salvo a mensagem no banco: {Message}", message);

            await _channel.BasicAckAsync(
                deliveryTag: ea.DeliveryTag,
                multiple: false,
                cancellationToken: _stoppingToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing message");
            await _channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true, cancellationToken: _stoppingToken);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_consumer is not null)
        {
            // Cancela a inscrição do consumidor para parar de receber novas mensagens
            _consumer.ReceivedAsync -= OnMessageReceived;
        }
        // Fecha o canal graciosamente quando a aplicação for desligada
        if (_channel is not null)
        {
            await _channel.CloseAsync(cancellationToken);
        }

        await base.StopAsync(cancellationToken);
    }
}
