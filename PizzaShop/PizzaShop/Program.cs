﻿using System.Text.Json;
using System.Threading.Channels;
using AsbGateway;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PizzaShop;

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
        //we use multiple pumps, because we don't want all channels to become unresponsive because one gets busy!!! In practice, we would
        //want competing consumers here
        services.AddHostedService<AsbMessagePumpService<Order>>(serviceProvider => AddHostedOrderService(hostContext, serviceProvider));
        
        //we have distinct queues for job accepted and job rejected to listen to each courier - all post to the same channel
        foreach (var courier in couriers)
        {
            //work around the problem of multiple service registration by using a singleton explicity, see https://github.com/dotnet/runtime/issues/38751
            services.AddSingleton<IHostedService, AsbMessagePumpService<JobAccepted>>(serviceProvider => AddHostedJobAcceptedService($"{courier}-job-accepted", hostContext, serviceProvider));
            services.AddSingleton<IHostedService, AsbMessagePumpService<JobRejected>>(serviceProvider => AddHostedJobRejectedService($"{courier}-job-rejected", hostContext, serviceProvider));
        }
        
        //We use channels for our internal pipeline. Channels let us easily wait on work without synchronization primitives
        services.AddHostedService<KitchenService>(_ => AddHostedKitchenService(hostContext));
        services.AddHostedService<DispatchService>(_ => AddHostedDispatcherService(hostContext));
    });

await hostBuilder.Build().RunAsync();

AsbMessagePumpService<Order> AddHostedOrderService(HostBuilderContext hostBuilderContext, IServiceProvider serviceProvider)
{
    var connectionString = hostBuilderContext.Configuration["ServiceBus:ConnectionString"];
    var queueName = hostBuilderContext.Configuration["ServiceBus:OrderQueueName"];
            
    if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(queueName))
    {
        throw new InvalidOperationException("ServiceBus:ConnectionString and ServiceBus:OrderQueueName must be set in configuration");
    }
            
    var client = new ServiceBusClient(connectionString);
    var logger = serviceProvider.GetRequiredService<ILogger<AsbMessagePump<Order>>>();
    return new AsbMessagePumpService<Order>(
        client, 
        queueName, 
        logger,
        message => JsonSerializer.Deserialize<Order>(message.Body.ToString()) ?? throw new InvalidOperationException("Invalid message"), 
        async (order, token) => await new PlaceOrderHandler(cookRequests, deliveryRequests, couriers).HandleAsync(order, token));
}

AsbMessagePumpService<JobAccepted> AddHostedJobAcceptedService(string queueName, HostBuilderContext hostBuilderContext, IServiceProvider serviceProvider1)
{
    var connectionString = hostBuilderContext.Configuration["ServiceBus:ConnectionString"];
            
    if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(queueName))
    {
        throw new InvalidOperationException("ServiceBus:ConnectionString and ServiceBus:JobAcceptedQueueName must be set in configuration");
    }
            
    var client = new ServiceBusClient(connectionString);
    var logger = serviceProvider1.GetRequiredService<ILogger<AsbMessagePump<JobAccepted>>>();
    return new AsbMessagePumpService<JobAccepted>(
        client, 
        queueName, 
        logger,
        message => JsonSerializer.Deserialize<JobAccepted>(message.Body.ToString()) ?? throw new InvalidOperationException("Invalid message"), 
        async (jobAccepted, token) => await new JobAcceptedHandler(courierStatusUpdates).HandleAsync(jobAccepted, token));
}

AsbMessagePumpService<JobRejected> AddHostedJobRejectedService(string queueName, HostBuilderContext hostBuilderContext, IServiceProvider serviceProvider2)
{
    var connectionString = hostBuilderContext.Configuration["ServiceBus:ConnectionString"];
            
    if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(queueName))
    {
        throw new InvalidOperationException("ServiceBus:ConnectionString and ServiceBus:JobRejectedQueueName must be set in configuration");
    }
            
    var client = new ServiceBusClient(connectionString);
    var logger = serviceProvider2.GetRequiredService<ILogger<AsbMessagePump<JobRejected>>>();
    return new AsbMessagePumpService<JobRejected>(
        client, 
        queueName, 
        logger,
        message => JsonSerializer.Deserialize<JobRejected>(message.Body.ToString()) ?? throw new InvalidOperationException("Invalid message"), 
        async (jobRejected, token) => await new JobRejectedHandler(courierStatusUpdates).HandleAsync(jobRejected, token));
}

KitchenService AddHostedKitchenService(HostBuilderContext hostBuilderContext)
{
    var connectionString = hostBuilderContext.Configuration["ServiceBus:ConnectionString"];
            
    if (string.IsNullOrEmpty(connectionString))
    {
        throw new InvalidOperationException("ServiceBus:ConnectionString must be set in configuration");
    }
            
    var orderProducer = new AsbProducer<OrderReady>(
        new ServiceBusClient(connectionString),
        message => new ServiceBusMessage(JsonSerializer.Serialize(message.Content)));
    
    var rejectedProducer = new AsbProducer<OrderRejected>(
        new ServiceBusClient(connectionString),
        message => new ServiceBusMessage(JsonSerializer.Serialize(message.Content)));
            
    return new KitchenService(cookRequests, courierStatusUpdates, orderProducer, rejectedProducer);
}

DispatchService AddHostedDispatcherService(HostBuilderContext hostBuilderContext)
{
    var connectionString = hostBuilderContext.Configuration["ServiceBus:ConnectionString"];

    if (string.IsNullOrEmpty(connectionString))
    {
        throw new InvalidOperationException("ServiceBus: ConnectionString must be set in configuration");
    }

    var deliveryManifestProducer = new AsbProducer<DeliveryManifest>(
        new ServiceBusClient(connectionString),
        message => new ServiceBusMessage(JsonSerializer.Serialize(message.Content)));

    return new DispatchService(deliveryRequests, deliveryManifestProducer);

}