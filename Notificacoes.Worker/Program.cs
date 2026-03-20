using MassTransit;
using Notificacoes.Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddMassTransit(x =>
{
    // 1. Avisamos ao MassTransit que este consumidor existe neste projeto
    x.AddConsumer<EmailNotificationConsumer>();

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.Host("localhost", "/", h => {
            h.Username("guest");
            h.Password("guest");
        });

        // 2. Configuração de Retry: 3 tentativas com intervalo de 5 segundos entre elas
        cfg.UseMessageRetry(r => r.Interval(3, TimeSpan.FromSeconds(2)));

        // 3. A mágica: manda o MassTransit ler os consumidores registrados e criar as filas/exchanges no RabbitMQ
        cfg.ConfigureEndpoints(context);
    });
});

var host = builder.Build();
host.Run();
