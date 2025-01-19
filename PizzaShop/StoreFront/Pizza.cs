﻿using System.ComponentModel;

namespace StoreFront;

/// <summary>
/// A pizza in an order. A pizza has a size and toppings
/// </summary>
public class Pizza
{
    public int Id { get; set; }
    [Description("What order does this pizza belong to?")]
    public int OrderId { get; set; }
    [Description("How large should this pizza be, in inches?")]
    public int Size { get; set; }
    [Description("What toppings should be on this pizza?")]
    public List<PizzaTopping> Toppings { get; set; } = new();
}