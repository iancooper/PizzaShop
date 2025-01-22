using System.Text.Json;
using System.Threading.Channels;
using AsbGateway;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PizzaShop;
using Shared;

//our pizza shop has internal channels for its orchestration
// -- cookrequests are sent to the kitchen
// -- deliveryrequests are sent to the dispatch service
// -- courierstatusupdates are sent to the kitchen

var cookRequests = Channel.CreateBounded<CookRequest>(10);
var deliveryRequests = Channel.CreateBounded<DeliveryRequest>(10);
var courierStatusUpdates = Channel.CreateBounded<CourierStatusUpdate>(10);

//our collection of couriers, names are used within queues & streams as well
string[] couriers = ["alice", "bob", "charlie"];

var hostBuilder = new HostBuilder()
    .ConfigureHostConfiguration((config) =>
    {
        config.AddEnvironmentVariables();
    })
    .ConfigureAppConfiguration((hostContext, config) =>
    {
        config.SetBasePath(Environment.CurrentDirectory);
        config.AddJsonFile("appsettings.json", optional: false);
        config.AddJsonFile($"appsettings.{hostContext.HostingEnvironment.EnvironmentName}.json", optional: true);
        config.AddEnvironmentVariables();
    })
    .ConfigureServices((hostContext, services) =>
    {
        services.AddPizzaShopTelemetry("PizzaShop");
        //we use multiple pumps, because we don't want all channels to become unresponsive because one gets busy!!! In practice, we would
        //want competing consumers here
        services.AddAzureClients(clientBuilder => {
            clientBuilder.AddServiceBusClient(hostContext.Configuration["ServiceBus:ConnectionString"]);
        });

        services.AddHostedService<AsbMessagePumpService<Order>>(serviceProvider => AddHostedOrderService(hostContext, serviceProvider));

        //we have distinct queues for job accepted and job rejected to listen to each courier - all post to the same channel
        foreach (var courier in couriers)
        {
            //work around the problem of multiple service registration by using a singleton explicity, see https://github.com/dotnet/runtime/issues/38751
            services.AddSingleton<IHostedService, AsbMessagePumpService<JobAccepted>>(serviceProvider => AddHostedJobAcceptedService($"{courier}-job-accepted", hostContext, serviceProvider));
            services.AddSingleton<IHostedService, AsbMessagePumpService<JobRejected>>(serviceProvider => AddHostedJobRejectedService($"{courier}-job-rejected", hostContext, serviceProvider));
        }
        
        //We use channels for our internal pipeline. Channels let us easily wait on work without synchronization primitives
        services.AddHostedService<KitchenService>(serviceProvider => AddHostedKitchenService(hostContext,serviceProvider));
        services.AddHostedService<DispatchService>(serviceProvider => AddHostedDispatcherService(hostContext, serviceProvider));
    });

await hostBuilder.Build().RunAsync();

AsbMessagePumpService<Order> AddHostedOrderService(HostBuilderContext hostBuilderContext, IServiceProvider serviceProvider)
{
    var queueName = hostBuilderContext.Configuration["ServiceBus:OrderQueueName"];
            
    if (string.IsNullOrEmpty(queueName))
    {
        throw new InvalidOperationException("ServiceBus:ConnectionString and ServiceBus:OrderQueueName must be set in configuration");
    }
            
    var client = serviceProvider.GetRequiredService<ServiceBusClient>();
    var logger = serviceProvider.GetRequiredService<ILogger<AsbMessagePump<Order>>>();
    return new AsbMessagePumpService<Order>(
        client, 
        queueName, 
        logger,
        message => JsonSerializer.Deserialize<Order>(message.Body.ToString()) ?? throw new InvalidOperationException("Invalid message"), 
        async (order, token) => await new PlaceOrderHandler(cookRequests, deliveryRequests, couriers).HandleAsync(order, token));
}

AsbMessagePumpService<JobAccepted> AddHostedJobAcceptedService(string queueName, HostBuilderContext hostBuilderContext, IServiceProvider serviceProvider)
{            
    if (string.IsNullOrEmpty(queueName))
    {
        throw new InvalidOperationException("ServiceBus:ConnectionString and ServiceBus:JobAcceptedQueueName must be set in configuration");
    }
            
    var client = serviceProvider.GetRequiredService<ServiceBusClient>();
    var logger = serviceProvider.GetRequiredService<ILogger<AsbMessagePump<JobAccepted>>>();
    return new AsbMessagePumpService<JobAccepted>(
        client, 
        queueName, 
        logger,
        message => JsonSerializer.Deserialize<JobAccepted>(message.Body.ToString()) ?? throw new InvalidOperationException("Invalid message"), 
        async (jobAccepted, token) => await new JobAcceptedHandler(courierStatusUpdates).HandleAsync(jobAccepted, token));
}

AsbMessagePumpService<JobRejected> AddHostedJobRejectedService(string queueName, HostBuilderContext hostBuilderContext, IServiceProvider serviceProvider)
{            
    if (string.IsNullOrEmpty(queueName))
    {
        throw new InvalidOperationException("ServiceBus:ConnectionString and ServiceBus:JobRejectedQueueName must be set in configuration");
    }
            
    var client = serviceProvider.GetRequiredService<ServiceBusClient>();
    var logger = serviceProvider.GetRequiredService<ILogger<AsbMessagePump<JobRejected>>>();
    return new AsbMessagePumpService<JobRejected>(
        client, 
        queueName, 
        logger,
        message => JsonSerializer.Deserialize<JobRejected>(message.Body.ToString()) ?? throw new InvalidOperationException("Invalid message"), 
        async (jobRejected, token) => await new JobRejectedHandler(courierStatusUpdates).HandleAsync(jobRejected, token));
}

KitchenService AddHostedKitchenService(HostBuilderContext hostBuilderContext, IServiceProvider serviceProvider)
{
    var client = serviceProvider.GetRequiredService<ServiceBusClient>();
            
    var orderProducer = new AsbProducer<OrderReady>(
        client,
        message => new ServiceBusMessage(JsonSerializer.Serialize(message.Content)));
    
    var rejectedProducer = new AsbProducer<OrderRejected>(
        client,
        message => new ServiceBusMessage(JsonSerializer.Serialize(message.Content)));
            
    return new KitchenService(cookRequests, courierStatusUpdates, orderProducer, rejectedProducer);
}

DispatchService AddHostedDispatcherService(HostBuilderContext hostBuilderContext, IServiceProvider serviceProvider)
{
    var client = serviceProvider.GetRequiredService<ServiceBusClient>();
    var deliveryManifestProducer = new AsbProducer<DeliveryManifest>(
        client,
        message => new ServiceBusMessage(JsonSerializer.Serialize(message.Content)));

    return new DispatchService(deliveryRequests, deliveryManifestProducer);

}