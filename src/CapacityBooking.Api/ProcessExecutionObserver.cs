using CapacityBooking.Application;
using CapacityBooking.Infrastructure.Reliability;

/// <summary>Opt-in Development-only OS-process failure seam for reproducible crash evidence.</summary>
public sealed class ProcessExecutionObserver(IConfiguration configuration, IHostEnvironment environment)
    : IExecutionObserver
{
    private int triggered;

    public async Task ReachedAsync(string point, string resourceId, CancellationToken ct = default)
    {
        if (!environment.IsDevelopment() || configuration["FailureInjection:Point"] != point) return;
        var resourceFilter = configuration["FailureInjection:ResourceId"];
        if (!string.IsNullOrEmpty(resourceFilter) && resourceFilter != resourceId) return;
        if (Interlocked.Exchange(ref triggered, 1) != 0) return;
        var signal = configuration["FailureInjection:SignalFile"];
        if (!string.IsNullOrEmpty(signal))
            await File.WriteAllTextAsync(signal, $"{point} {resourceId}", ct);
        Environment.Exit(86); // Intentionally bypass disposal; recovery must use persisted state.
    }
}

public sealed class DemonstrationTransport(LocalMessageTransport inner, IConfiguration configuration,
    IHostEnvironment environment) : IMessageTransport
{
    public Task PublishAsync(BookingConfirmedMessage message, CancellationToken ct = default)
    {
        if (environment.IsDevelopment() && configuration.GetValue<bool>("DemoTransport:Unavailable"))
            throw new IOException("Demonstration transport is unavailable.");
        return inner.PublishAsync(message, ct);
    }
}
