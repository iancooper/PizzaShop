﻿using System.ComponentModel.DataAnnotations;

namespace PizzaShop;

public class Address
{
    public int Id { get; set; }
		
    public string Name { get; set; } = string.Empty;

    public string Line1 { get; set; } = string.Empty;

    public string City { get; set; } = string.Empty;

    public string PostalCode { get; set; } = string.Empty;
}
