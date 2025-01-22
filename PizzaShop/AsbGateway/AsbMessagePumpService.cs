using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AsbGateway;

public class AsbMessagePumpService<T>(
    ServiceBusClient client,
    string queueName,
    ILogger<AsbMessagePump<T>> logger,
    Func<ServiceBusReceivedMessage, T> mapToRequest,
    Func<T,CancellationToken,Task<bool>> handler)
    : BackgroundService 
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Starting message pump for {queueName}", queueName);
        try {
            var messagePump = new AsbMessagePump<T>(client, queueName, mapToRequest, handler, logger);
            await messagePump.Run(stoppingToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error starting message pump");
            await Task.Delay(1000, stoppingToken);
        }
    }
}