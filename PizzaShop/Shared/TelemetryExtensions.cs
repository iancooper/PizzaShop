using Confluent.Kafka.Extensions.OpenTelemetry;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Shared;

public static class TelemetryExtensions
{
    public static IServiceCollection AddPizzaShopTelemetry(this IServiceCollection services, string serviceName, string[]? additionalSources = null, Dictionary<string, string>? additionalResourceAttributes = null)
    {
        services.AddOpenTelemetry()
            .UseOtlpExporter()
            .ConfigureResource(builder => builder.AddService(serviceName))
            .WithTracing(builder => {
                builder
                .AddSource(serviceName)
                .AddSource(AsbGateway.DiagnosticSettings.Source.Name)
                .AddAspNetCoreInstrumentation()
                .AddConfluentKafkaInstrumentation();

                if (additionalSources != null)
                {
                    foreach (var source in additionalSources)
                    {
                        builder.AddSource(source);
                    }
                }
            })
            .WithMetrics()
            .WithLogging();

        
        AppContext.SetSwitch("Azure.Experimental.EnableActivitySource", true);
        services.AddOpenTelemetry().WithTracing(builder => builder.AddSource("Azure.Messaging.ServiceBus.*"));

        return services;
    }

}
