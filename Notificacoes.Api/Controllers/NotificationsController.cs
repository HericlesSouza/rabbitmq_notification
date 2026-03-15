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
            Id = Guid.Parse("e9e7d767-563d-47fb-a2b8-952641bf924c"),
            To = request.To,
            Subject = request.Subject,
            Body = request.Body,
            CreatedAt = DateTime.UtcNow
        };

        await producer.ProduceAsync(message);

        return Accepted(new { message.Id, Status = "Notificação enfileirada com sucesso." });
    }
}
