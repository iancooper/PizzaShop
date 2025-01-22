using System.Text.Json;
using System.Threading.Channels;
using AsbGateway;
using Azure.Messaging.ServiceBus;
using Confluent.Kafka;
using Courier;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shared;

var deliveryJobs = Channel.CreateBounded<OrderStatus>(10);

var hostBuilder = new HostApplicationBuilder();
hostBuilder.Services.AddPizzaShopTelemetry("Courier", 
   additionalResourceAttributes: new Dictionary<string, string> { 
    { "pizza.courier", hostBuilder.Configuration.GetValue<string>("Courier:Name") ?? "unknown-courier" } 
    });

hostBuilder.Services.AddHostedService<AsbMessagePumpService<DeliveryManifest>>(serviceProvider => AddHostedAvailabilityRequestService(hostBuilder.Configuration, serviceProvider));
hostBuilder.Services.AddHostedService<AsbMessagePumpService<OrderReady>>(serviceProvider => AddHostedOrderReadyService(hostBuilder.Configuration, serviceProvider));
hostBuilder.Services.AddHostedService<DeliveryService>(serviceProvider => AddHostedDispatcherService(hostBuilder.Configuration, serviceProvider));

hostBuilder.AddKafkaProducer<int, string>("courier-order-status");

await hostBuilder.Build().RunAsync();

AsbMessagePumpService<DeliveryManifest> AddHostedAvailabilityRequestService(ConfigurationManager configurationManager, IServiceProvider serviceProvider)
{
    var connectionString = configurationManager.GetValue<string>("ServiceBus:ConnectionString");
    var courierName = configurationManager.GetValue<string>("Courier:Name");
            
    if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(courierName))
    {
        throw new InvalidOperationException("ServiceBus:ConnectionString and Courier:Name must be set in configuration");
    }
            
    var client = new ServiceBusClient(connectionString);
    var logger = serviceProvider.GetRequiredService<ILogger<AsbMessagePump<DeliveryManifest>>>();

    var acceptedProducer = new AsbProducer<JobAccepted>(
        new ServiceBusClient(connectionString), 
        message => new ServiceBusMessage(JsonSerializer.Serialize(message.Content))
    );

    var rejectedProducer = new AsbProducer<JobRejected>(
        new ServiceBusClient(connectionString),
        message => new ServiceBusMessage(JsonSerializer.Serialize(message.Content))
    );
            
    return new AsbMessagePumpService<DeliveryManifest>(
        client, 
        courierName + "-availability", 
        logger,
        message => JsonSerializer.Deserialize<DeliveryManifest>(message.Body.ToString()) ?? throw new InvalidOperationException("Invalid DeliveryManifest message"), 
        async (job, token) => await new AvailabilityRequestHandler(courierName, acceptedProducer, rejectedProducer).HandleAsync(job, token));
}

AsbMessagePumpService<OrderReady> AddHostedOrderReadyService(ConfigurationManager configurationManager, IServiceProvider serviceProvider)
{
    var connectionString = configurationManager.GetValue<string>("ServiceBus:ConnectionString");
    var courierName = configurationManager.GetValue<string>("Courier:Name");
            
    if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(courierName))
    {
        throw new InvalidOperationException("ServiceBus:ConnectionString and Courier:Name must be set in configuration");
    }
            
    var client = new ServiceBusClient(connectionString);
    var logger = serviceProvider.GetRequiredService<ILogger<AsbMessagePump<OrderReady>>>();
    
    return new AsbMessagePumpService<OrderReady>(
        client, 
        courierName + "-order-ready", 
        logger,
        message => JsonSerializer.Deserialize<OrderReady>(message.Body.ToString()) ?? throw new InvalidOperationException("Invalid OrderReady message"), 
        async (job, token) => await new OrderReadyHandler(courierName, deliveryJobs).HandleAsync(job, token));
}

DeliveryService AddHostedDispatcherService(ConfigurationManager configurationManager, IServiceProvider serviceProvider)
{
    var connectionString = configurationManager.GetValue<string>("ServiceBus:ConnectionString");
    var courierName = configurationManager.GetValue<string>("Courier:Name");

    if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(courierName))
    {
        throw new InvalidOperationException("ServiceBus: ConnectionString and ServiceBus:DeliveryManifestQueueName must be set in configuration");
    }

    var orderStatusProducer = serviceProvider.GetRequiredService<IProducer<int, string>>(); 

    return new DeliveryService(courierName + "-order-status", deliveryJobs, orderStatusProducer );

}