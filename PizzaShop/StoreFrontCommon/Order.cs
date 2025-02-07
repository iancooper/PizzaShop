﻿using System.ComponentModel;

namespace StoreFrontCommon;

/// <summary>
/// An order for our pizza shop
/// </summary>
public class Order
{
    [Description("The unique identifier for this order")]
    public int OrderId { get; set; }
    [Description("What time was the order placed?")]
    public DateTimeOffset CreatedTime { get; set; }
    [Description("Where do you want this delivered?")]
    public Address? DeliveryAddress { get; set; }
    [Description("What pizzas do you want?")]
    public ICollection<Pizza> Pizzas { get; set; } = new List<Pizza>();
    [Description("What is the status of this order?")]
    public DeliveryStatus Status { get; set; } = DeliveryStatus.Pending;
}
