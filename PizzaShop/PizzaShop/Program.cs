﻿using System.Text.Json;
using System.Threading.Channels;
using AsbGateway;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PizzaShop;

//our pizza shop has a channel for incoming orders and a channel for cook requests
var cookRequests = Channel.CreateBounded<CookRequest>(10);

//our pizza shop has a channel for incoming orders and a channel for delivery requests
var deliveryRequests = Channel.CreateBounded<DeliveryRequest>(10);

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
        services.AddHostedService<AsbMessagePumpService<Order>>(serviceProvider =>
        {
            var connectionString = hostContext.Configuration["ServiceBus:ConnectionString"];
            var queueName = hostContext.Configuration["ServiceBus:QueueName"];
            
            if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(queueName))
            {
                throw new InvalidOperationException("ServiceBus:ConnectionString and ServiceBus:QueueName must be set in configuration");
            }
            
            var client = new ServiceBusClient(connectionString);
            var logger = serviceProvider.GetRequiredService<ILogger<AsbMessagePump<Order>>>();
            return new AsbMessagePumpService<Order>(
                client, 
                queueName, 
                logger,
                message => JsonSerializer.Deserialize<Order>(message.Body.ToString()) ?? throw new InvalidOperationException("Invalid message"), 
                async (order, token) => await new PlaceOrderHandler(cookRequests, deliveryRequests).HandleAsync(order, token));
        });
        
        services.AddHostedService<KitchenService>(serviceProvider => new KitchenService(cookRequests));
        services.AddHostedService<DispatchService>(serviceProvider => new DispatchService(deliveryRequests));
    });

await hostBuilder.Build().RunAsync();