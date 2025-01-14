﻿using System.Text.Json;
using AsbGateway;
using Azure.Messaging.ServiceBus;
using Courier;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

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
        services.AddHostedService<AsbMessagePumpService<DeliveryManifest>>(serviceProvider =>
        {
            var connectionString = hostContext.Configuration["ServiceBus:ConnectionString"];
            var queueName = hostContext.Configuration["ServiceBus:QueueName"];
            
            if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(queueName))
            {
                throw new InvalidOperationException("ServiceBus:ConnectionString and ServiceBus:QueueName must be set in configuration");
            }
            
            var client = new ServiceBusClient(connectionString);
            var logger = serviceProvider.GetRequiredService<ILogger<AsbMessagePump<DeliveryManifest>>>();
            return new AsbMessagePumpService<DeliveryManifest>(
                client, 
                queueName, 
                logger,
                message => JsonSerializer.Deserialize<DeliveryManifest>(message.Body.ToString()) ?? throw new InvalidOperationException("Invalid message"), 
                async (job, token) => await new DeliveryJobHandler().Handle(job, token));
        });
    });

await hostBuilder.Build().RunAsync();