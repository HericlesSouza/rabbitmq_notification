using MassTransit;
using Microsoft.AspNetCore.Mvc;
using Notificacoes.Api.Models;

namespace Notificacoes.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class NotificationsController(IPublishEndpoint publishEndpoint) : Controller
{
    [HttpPost]
    public async Task<IActionResult> Post([FromBody] NotificationRequest request)
    {
        System.Console.WriteLine("chegou");

        var message = new NotificationMessage
        {
            Id = Guid.NewGuid(),
            To = request.To,
            Subject = request.Subject,
            Body = request.Body,
            CreatedAt = DateTime.UtcNow
        };
        
        await publishEndpoint.Publish(message);

        return Accepted(new { message.Id, Status = "Notificação enfileirada com sucesso." });
    }
}
