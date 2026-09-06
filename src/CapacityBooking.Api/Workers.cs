using CapacityBooking.Application;

public abstract class PollingWorker(IServiceScopeFactory scopes, IConfiguration configuration,
    ILogger logger) : BackgroundService
{
    protected abstract Task PollAsync(IServiceProvider services, CancellationToken ct);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Read inside ExecuteAsync so WebApplicationFactory can override configuration before startup.
        if (!configuration.GetValue("Workers:Enabled", true)) return;
        var interval = Math.Clamp(configuration.GetValue("Workers:PollIntervalMilliseconds", 250), 25, 60_000);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                await PollAsync(scope.ServiceProvider, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception error)
            {
                logger.LogError("Worker iteration failed; ErrorType={ErrorType}", error.GetType().Name);
            }
            try { await Task.Delay(interval, stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
        }
    }
}

public sealed class ExpiryWorker(IServiceScopeFactory scopes, IConfiguration configuration,
    ILogger<ExpiryWorker> logger) : PollingWorker(scopes, configuration, logger)
{
    protected override async Task PollAsync(IServiceProvider services, CancellationToken ct) =>
        _ = await services.GetRequiredService<IExpiryService>().ExpireDueAsync(ct: ct);
}

public sealed class OutboxWorker(IServiceScopeFactory scopes, IConfiguration configuration,
    ILogger<OutboxWorker> logger) : PollingWorker(scopes, configuration, logger)
{
    protected override async Task PollAsync(IServiceProvider services, CancellationToken ct) =>
        _ = await services.GetRequiredService<IOutboxPublisher>().PublishBatchAsync(ct: ct);
}
