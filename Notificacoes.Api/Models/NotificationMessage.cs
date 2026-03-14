namespace Notificacoes.Api.Models;

public class NotificationMessage
{
    public Guid Id { get; set; }
    public required string To { get; set; }
    public required string Subject { get; set; }
    public required string Body { get; set; }
    public DateTime CreatedAt { get; set; }
}
