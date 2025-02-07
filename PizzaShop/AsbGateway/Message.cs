using Azure.Messaging.ServiceBus;

namespace AsbGateway;

public class Message<T>(T content)
{
    public T Content { get; } = content;
}