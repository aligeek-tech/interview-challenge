using CapacityBooking.Application;
using CapacityBooking.Infrastructure;
using Microsoft.Extensions.Options;

public sealed class ServiceHealth(DatabaseReadinessProbe database, WorkerHealthRegistry workers,
    IConfiguration configuration, IOptions<HealthOptions> options)
{
    public async Task<ServiceHealthSnapshot> CheckAsync(CancellationToken ct = default)
    {
        var settings = options.Value;
        var db = await database.CheckAsync(ct);
        var workerStates = workers.Snapshot(configuration.GetValue("Workers:Enabled", true), settings.WorkerStaleAfter);
        var ready = db.Ready && workerStates.All(w => w.Status is "healthy" or "disabled");
        var degraded = db.QuarantinedOutbox > 0 || db.OldestOutboxAgeSeconds > settings.BacklogWarningAge.TotalSeconds ||
            db.OldestDueHoldAgeSeconds > settings.BacklogWarningAge.TotalSeconds;
        return new(ready ? degraded ? "degraded" : "healthy" : "unhealthy", ready, db, workerStates);
    }
}

public sealed record ServiceHealthSnapshot(string Status, bool Ready, DatabaseHealthSnapshot Database,
    IReadOnlyList<WorkerHealthSnapshot> Workers);
