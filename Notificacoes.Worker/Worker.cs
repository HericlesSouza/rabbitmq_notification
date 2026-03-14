using System.Text;
using System.Text.Json;
using Notificacoes.Worker.Models;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Notificacoes.Worker;

public class Worker(IConnection connection, ILogger<Worker> logger) : BackgroundService
{
    private const string QueueName = "email_notifications_queue";
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

        // Configura DLQ
        await _channel.ExchangeDeclareAsync(
            exchange: DlxName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null,
            cancellationToken: _stoppingToken
        );

        await _channel.QueueDeclareAsync(
            queue: DlqName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: _stoppingToken
        );

        await _channel.QueueBindAsync(
            queue: DlqName,
            exchange: DlxName,
            routingKey: RoutingKey,
            cancellationToken: _stoppingToken
        );

        var queueArgs = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", DlxName },
            { "x-dead-letter-routing-key", RoutingKey }
        };

        // Declara Fila principal
        await _channel.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs,
            cancellationToken: _stoppingToken);

        await _channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 10,
            global: false,
            cancellationToken: _stoppingToken);

        _consumer = new AsyncEventingBasicConsumer(_channel);

        _consumer.ReceivedAsync += OnMessageReceived;

        await _channel.BasicConsumeAsync(
            queue: QueueName,
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
            var notification = JsonSerializer.Deserialize<NotificationMessage>(message);

            logger.LogInformation("Nova notificação recebida para: {To} | Assunto: {Subject}", notification?.To, notification?.Subject);

            // Simulando o tempo de envio de um e-mail real...
            await Task.Delay(1000, _stoppingToken);

            logger.LogInformation("Notificação {Id} processada com sucesso!", notification?.Id);

            // 4. Confirma para o RabbitMQ que processou com sucesso (ACK)
            await _channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: _stoppingToken);
        }
        catch (JsonException jsonEx)
        {
            logger.LogError(jsonEx, "Erro ao desserializar a mensagem: {Message}", jsonEx.Message);
            // NACK sem requeue para mensagens malformadas
            await _channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: _stoppingToken);
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
