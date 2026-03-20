using MassTransit;
using Notificacoes.Api.Models;

namespace Notificacoes.Worker;

public class EmailNotificationConsumer(ILogger<EmailNotificationConsumer> logger) : IConsumer<NotificationMessage>
{
    public async Task Consume(ConsumeContext<NotificationMessage> context)
    {
        var notification = context.Message;

        logger.LogInformation("Nova notificação recebida para: {To} | Assunto: {Subject}", notification.To, notification.Subject);

        // Simulando o tempo de envio de um e-mail real...
        await Task.Delay(1000);

        // Lógica de negócio aqui...
        if (notification.To.Contains("@"))
            throw new Exception("Deu ruim!");

        logger.LogInformation("Notificação {Id} processada com sucesso!", notification?.Id);
    }
}