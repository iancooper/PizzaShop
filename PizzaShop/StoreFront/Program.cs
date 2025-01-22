using System.ComponentModel;
using System.Text.Json;
using AsbGateway;
using Azure.Messaging.ServiceBus;
using Confluent.Kafka;
using KafkaGateway;
using Microsoft.EntityFrameworkCore;
using Shared;
using StoreFront;
using StoreFront.Seed;

//our collection of couriers, names are used within queues & streams as well
string[] couriers = ["alice", "bob", "charlie"];

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddPizzaShopTelemetry("StoreFront");

builder.Services.AddDbContext<PizzaShopDb>(opt => opt.UseInMemoryDatabase("TodoList"));
builder.AddKafkaConsumer<int, string>("courier-order-status", settings =>
{
    settings.Config.GroupId = "storefront-consumer-group";
    settings.Config.AutoOffsetReset = AutoOffsetReset.Earliest;
    settings.Config.BootstrapServers = "localhost:9092";
    settings.Config.EnableAutoOffsetStore = true;
    settings.Config.EnableAutoCommit = false;
});

// Listens to status updates about an order
// Normally, we would tend to run a Kafka worker in a separate process, so that we could scale out to the number of
// partitions we had, separate to scaling for the number of HTTP requests.
// To make this simpler, for now, we are just running it as a background process, as we don't need to scale it

foreach (var courier in couriers)
{
    //work around the problem of multiple service registration by using a singleton explicity, see https://github.com/dotnet/runtime/issues/38751
    builder.Services.AddSingleton<IHostedService, KafkaMessagePumpService<int, string>>(serviceProvider => AddAfterOrderService(courier + "-order-status", serviceProvider) );
}

builder.Services.AddOpenApi();

var app = builder.Build();

var scopeFactory = app.Services.GetRequiredService<IServiceScopeFactory>();
using (var scope = scopeFactory.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<PizzaShopDb>();
    if (db.Database.EnsureCreated())
    {
        await MenuMaker.CreateToppings(db);
        await OrderMaker.DeliveredOrders(db);
    }
}

app.MapOpenApi();

app.MapGet("/orders", async (PizzaShopDb db) =>
    {
        return await db.Orders
            .Include(o => o.Pizzas)
            .ThenInclude(p => p.Toppings)
            .ThenInclude(t => t.Topping)
            .ToListAsync();
    })
    .WithSummary("Retrieve all orders")
    .WithDescription("The orders endpoint allows you to retrieve all orders.");

app.MapGet("/orders/pending", async (PizzaShopDb db) =>
    {
        return await db.Orders.Where(o => o.Status == OrderStatus.Pending)
                .Include(o => o.Pizzas)
                .ThenInclude(p => p.Toppings)
                .ThenInclude(t => t.Topping)
                .ToListAsync();
    })
    .WithSummary("Retrieve all pending orders")
        .WithDescription("The pending orders endpoint allows you to retrieve all orders that are currently pending.");


app.MapGet("/orders/{id}", async ([Description("The id of the order to watch")] int id, PizzaShopDb db) =>
    {
        var order =  await db.Orders
                .Where(o => o.OrderId == id)
                .Include(o => o.Pizzas)
                .ThenInclude(p => p.Toppings)
                .ThenInclude(t => t.Topping)
                .SingleOrDefaultAsync();
        
        return order != null
            ? Results.Ok(order)
            : Results.NotFound();
    })
    .WithSummary("Retrieve an order by its ID")
    .WithDescription("The order endpoint allows you to retrieve an order by its ID.");

app.MapPost("/orders", async ([Description("The pizza order you wish to make")]Order order, PizzaShopDb db) =>
{
    order.CreatedTime = DateTimeOffset.UtcNow;
    
    //we already have the toppings, so we must attach them to the order
    foreach (var pizza in order.Pizzas)
    {
        foreach (var topping in pizza.Toppings)
        {
            topping.ToppingId = topping.Topping?.ToppingId ?? 0;
            topping.Topping = null;
        }
    }
    
    db.Orders.Attach(order);
    
    //really we should use an Outbox pattern here, to save the outgoing message, but for now we will just save the order
    await db.SaveChangesAsync();

    await SendOrderAsync(order);

    //get the order we just saved
    var pendingOrder = await db.Orders
        .Where(o => o.OrderId == order.OrderId)
        .Include(o => o.Pizzas)
        .ThenInclude(p => p.Toppings)
        .ThenInclude(t => t.Topping)
        .SingleOrDefaultAsync();

    return Results.Accepted($"/orders/{order.OrderId}", pendingOrder);
})
.WithSummary("Allows new orders to be raised")
.WithDescription("The new orders endpoint is intended to allow orders to be raised. Orders are created with a status of 'Pending' and an ETA of 30 minutes.");

app.MapGet("/toppings", async (PizzaShopDb db) => await db.Toppings.ToListAsync())
    .WithSummary("Retrieve all toppings")
    .WithDescription("The toppings endpoint allows you to retrieve all toppings.");

//needs a background service that updates the status of the order in response to Kafka messages, which you can see via
//polling the page returned in the 204 Accept

await app.RunAsync();

async Task SendOrderAsync(Order order)
{
    var connectionString = app.Configuration.GetValue<string>("ServiceBus:ConnectionString");
    var queueName = app.Configuration.GetValue<string>("ServiceBus:OrderQueueName");
            
    if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(queueName))
    {
        throw new InvalidOperationException("ServiceBus:ConnectionString and ServiceBus:OrderQueueName must be set in configuration");
    }
            
    var client = new ServiceBusClient(connectionString);
    var orderProducer = new AsbProducer<Order>(
        client, 
        message => new ServiceBusMessage(JsonSerializer.Serialize(message.Content)));
    await orderProducer.SendMessageAsync(queueName, new Message<Order>(order));
}

KafkaMessagePumpService<int, string> AddAfterOrderService(string queueName, IServiceProvider serviceProvider)
{
    var consumer = serviceProvider.GetService<IConsumer<int, string>>();
    if (consumer is null) throw new InvalidOperationException("No Kafka Consumer registered");
    
    var logger = serviceProvider.GetRequiredService<ILogger<KafkaMessagePump<int, string>>>();
    
    return new KafkaMessagePumpService<int, string>(
        consumer, 
        queueName, 
        logger,
        (async (key, value) =>
        {
            var orderId = key;
            var orderStatus = Enum.Parse<OrderStatus>(value);
            
            {
                var db = serviceProvider.GetService<PizzaShopDb>();
                if (db is null) throw new InvalidOperationException("No  EF Context");
                
                var orderToUpdate = await db.Orders.SingleOrDefaultAsync(o => o.OrderId == orderId);
                if (orderToUpdate == null)
                {
                    return false; 
                }
                orderToUpdate.Status = orderStatus;
                await db.SaveChangesAsync();
                return true;
            }
        }));
}