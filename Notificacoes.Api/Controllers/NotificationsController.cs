using Microsoft.AspNetCore.Mvc;
using Notificacoes.Api.Messaging;
using Notificacoes.Api.Models;

namespace Notificacoes.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class NotificationsController(NotificationProducer producer) : Controller
{
    [HttpPost]
    public async Task<IActionResult> Post([FromBody] NotificationRequest request)
    {
        var message = new NotificationMessage
        {
            Id = Guid.NewGuid(),
            To = request.To,
            Subject = request.Subject,
            Body = request.Body,
            CreatedAt = DateTime.UtcNow
        };

        await producer.ProduceAsync(message);

        return Accepted(new { message.Id, Status = "Notificação enfileirada com sucesso." });
    }
}
