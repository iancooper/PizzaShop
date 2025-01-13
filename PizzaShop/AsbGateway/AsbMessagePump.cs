using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;

namespace AsbGateway;

/// <summary>
/// Creates a message pump that will read, translate and dispatch a message from an ASB queue
/// </summary>
/// <param name="busClient">The client for Azure Service Bus</param>
/// <param name="queueName">The queue we are reading</param>
/// <param name="mapToRequest">A function to map a ServiceBusMessage to a <see cref="Request{T}"/> type</param>
/// <param name="handler">A function to handle the content of the <see cref="Request{T}"/> type</param>
/// <typeparam name="T"></typeparam>
public class AsbMessagePump<T>(
    ServiceBusClient busClient,
    string queueName,
    Func<ServiceBusReceivedMessage, T> mapToRequest,
    Func<T, CancellationToken, Task<bool>> handler,
    ILogger<AsbMessagePump<T>> logger)
{

    public async Task Run(CancellationToken cancellationToken = default)
    {
        var processor = busClient.CreateProcessor(queueName, new ServiceBusProcessorOptions());
        processor.ProcessMessageAsync += async args =>
        {
            var request = mapToRequest(args.Message);
            var result = await handler(request, cancellationToken);
            if (result)
            {
                await args.CompleteMessageAsync(args.Message, cancellationToken );
            }
            else
            {
                await args.AbandonMessageAsync(args.Message, cancellationToken: cancellationToken);
            }
        };
        processor.ProcessErrorAsync += args =>
        {
            logger.LogError(args.Exception, "Error processing message");
            return Task.CompletedTask;
        };
        await processor.StartProcessingAsync(cancellationToken);
    }
}