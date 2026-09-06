using CapacityBooking.Application;

public abstract class PollingWorker(IServiceScopeFactory scopes, IConfiguration configuration,
    WorkerHealthRegistry health, ILogger logger) : BackgroundService
{
    protected abstract string WorkerName { get; }
    protected abstract Task PollAsync(IServiceProvider services, CancellationToken ct);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Read inside ExecuteAsync so WebApplicationFactory can override configuration before startup.
        var enabled = configuration.GetValue("Workers:Enabled", true);
        health.Started(WorkerName, enabled);
        if (!enabled) return;
        var interval = Math.Clamp(configuration.GetValue("Workers:PollIntervalMilliseconds", 250), 25, 60_000);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    health.PollStarted(WorkerName);
                    await using var scope = scopes.CreateAsyncScope();
                    await scope.ServiceProvider.GetRequiredService<IExecutionObserver>()
                        .ReachedAsync($"worker.{WorkerName}.before-poll", WorkerName, stoppingToken);
                    await PollAsync(scope.ServiceProvider, stoppingToken);
                    health.PollSucceeded(WorkerName);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                catch (Exception error)
                {
                    health.PollFailed(WorkerName, error);
                    logger.LogError("Worker {Worker} iteration failed; ErrorType={ErrorType}", WorkerName, error.GetType().Name);
                }
                try { await Task.Delay(interval, stoppingToken); }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            }
        }
        finally { health.Stopped(WorkerName); }
    }
}

public sealed class ExpiryWorker(IServiceScopeFactory scopes, IConfiguration configuration,
    WorkerHealthRegistry health, ILogger<ExpiryWorker> logger) : PollingWorker(scopes, configuration, health, logger)
{
    private readonly ExpirySweepState _sweep = new();
    protected override string WorkerName => "expiry";
    protected override async Task PollAsync(IServiceProvider services, CancellationToken ct)
    {
        var result = await services.GetRequiredService<IExpiryService>().SweepDueAsync(_sweep, ct: ct);
        if (result.BudgetExhausted && result.ResolvedCandidates == 0 && !result.SweepCompleted)
            throw new TimeoutException("Expiry exhausted its poll budget without resolving any work.");
    }
}

public sealed class OutboxWorker(IServiceScopeFactory scopes, IConfiguration configuration,
    WorkerHealthRegistry health, ILogger<OutboxWorker> logger) : PollingWorker(scopes, configuration, health, logger)
{
    protected override string WorkerName => "outbox";
    protected override async Task PollAsync(IServiceProvider services, CancellationToken ct) =>
        _ = await services.GetRequiredService<IOutboxPublisher>().PublishBatchAsync(ct: ct);
}
