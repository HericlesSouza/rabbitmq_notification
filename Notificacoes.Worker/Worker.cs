using System.Text;
using System.Text.Json;
using Notificacoes.Worker.Models;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Notificacoes.Worker;

public class Worker(IConnection connection, ILogger<Worker> logger) : BackgroundService
{
    private const string QueueName = "email_notifications_queue";

    // Configurações DLQ (Erros Permanentes)
    private const string DlxName = "email_notifications_dlx";
    private const string DlqName = "email_notifications_dlq";
    private const string RoutingKey = "error.email";

    // Configurações Retry (Erros Temporários)
    private const string RetryExchange = "email_notifications_retry_exchange";
    private const string RetryQueue = "email_notifications_retry_queue";

    private CancellationToken _stoppingToken = default;
    private AsyncEventingBasicConsumer? _consumer;
    private IChannel? _channel;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;

        _channel = await connection.CreateChannelAsync(cancellationToken: _stoppingToken);

        // 1. Declaração da DLX e DLQ
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

        // 2. Declaração do Retry
        await _channel.ExchangeDeclareAsync(
            exchange: RetryExchange,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: _stoppingToken
        );

        var retryArgs = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", "" }, // A string vazia representa a exchange padrão do RabbitMQ
            { "x-dead-letter-routing-key", QueueName }, // O CEP de volta é o nome da nossa fila principal!
            { "x-message-ttl", 5000 } // A mensagem morre aqui após 5000 ms (5 segundos)
        };

        await _channel.QueueDeclareAsync(
            queue: RetryQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: retryArgs,
            cancellationToken: _stoppingToken
        );

        // 3. Declaração da Fila Principal
        await _channel.QueueBindAsync(
            queue: RetryQueue,
            exchange: RetryExchange,
            routingKey: QueueName,
            cancellationToken: _stoppingToken);

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

        // 4. Configuração do Consumidor        
        await _channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 20,
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


            if (notification.To.Contains("@"))
                throw new Exception("Deu ruim!");


            logger.LogInformation("Notificação {Id} processada com sucesso!", notification?.Id);

            // 4. Confirma para o RabbitMQ que processou com sucesso (ACK)
            await _channel!.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: _stoppingToken);
        }
        catch (JsonException jsonEx)
        {
            logger.LogError(jsonEx, "Erro ao desserializar a mensagem: {Message}", jsonEx.Message);
            // NACK sem requeue para mensagens malformadas
            await _channel!.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: _stoppingToken);
        }
        catch (Exception ex)
        {
            // --- LÓGICA DE RETRY ---
            const int MaxRetries = 3;

            // 1. Tenta obter o contador atual do cabeçalho "x-retry-count"
            int currentRetry = 0;
            if (ea.BasicProperties.Headers != null && ea.BasicProperties.Headers.ContainsKey("x-retry-count"))
            {
                currentRetry = (int)ea.BasicProperties.Headers["x-retry-count"]!;
            }

            if (currentRetry < MaxRetries)
            {
                currentRetry++;
                logger.LogWarning("Tentativa {Count} de {Max} falhou. Reenviando para Retry Queue...", currentRetry, MaxRetries);

                // 2. Cria novas propriedades clonando as antigas e atualizando o header
                var properties = new BasicProperties
                {
                    DeliveryMode = DeliveryModes.Persistent,
                    Headers = ea.BasicProperties.Headers ?? new Dictionary<string, object?>()
                };
                properties.Headers["x-retry-count"] = currentRetry;

                // 3. Publica na Exchange de Retry
                await _channel!.BasicPublishAsync(
                    exchange: RetryExchange,
                    routingKey: QueueName,
                    mandatory: true,
                    basicProperties: properties,
                    body: ea.Body,
                    cancellationToken: _stoppingToken);

                // 4. Dá ACK na mensagem atual para removê-la da fila principal (a cópia está na retry)
                await _channel!.BasicAckAsync(ea.DeliveryTag, false, _stoppingToken);
            }
            else
            {
                logger.LogError("Limite de {Max} retentativas atingido. Movendo para DLQ.", MaxRetries);

                // 5. NACK com requeue: false envia para a DLX configurada na declaração da fila principal
                await _channel!.BasicNackAsync(ea.DeliveryTag, false, false, _stoppingToken);
            }
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
