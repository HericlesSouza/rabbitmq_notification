using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Notificacoes.WorkerAudit;

public class Worker(IConnection connection, ILogger<Worker> logger) : BackgroundService
{
    private const string QueueName = "audit_notifications_queue";
    private const string ExchangeNotification = "notifications_exchange";
    private AsyncEventingBasicConsumer? _consumer;
    private IChannel? _channel;
    private CancellationToken _stoppingToken = default;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;

        _channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await _channel.ExchangeDeclareAsync(
            exchange: ExchangeNotification,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: _stoppingToken
        );

        await _channel.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: _stoppingToken
        );

        await _channel.QueueBindAsync(
            queue: QueueName,
            exchange: ExchangeNotification,
            routingKey: "notification.#",
            cancellationToken: _stoppingToken
        );


        _consumer = new AsyncEventingBasicConsumer(_channel);

        _consumer.ReceivedAsync += OnMessageReceived;

        await _channel.BasicConsumeAsync(
            queue: QueueName,
            autoAck: false,
            consumer: _consumer,
            cancellationToken: _stoppingToken
        );
    }

    private async Task OnMessageReceived(object sender, BasicDeliverEventArgs ea)
    {
        try
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            if (message == null) return;

            logger.LogInformation("Nova mensagem no Worker Audit");

            await Task.Delay(1000, _stoppingToken);

            logger.LogInformation("Salvando no banco de dados, a mensagem: {message}!", message);

            await _channel!.BasicAckAsync(ea.DeliveryTag, false, _stoppingToken);
        }
        catch (Exception ex)
        {
            logger.LogError("Ocorreu um erro: {Ex}", ex);

            // 5. NACK com requeue: false envia para a DLX configurada na declaração da fila principal
            await _channel!.BasicNackAsync(ea.DeliveryTag, false, false, _stoppingToken);
        }
    }
}
