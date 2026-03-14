using Notificacoes.WorkerDLQ;
using RabbitMQ.Client;

var builder = Host.CreateApplicationBuilder(args);

var factory = new ConnectionFactory {HostName = "localhost"};
var connection = await factory.CreateConnectionAsync();

builder.Services.AddSingleton(connection);

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
