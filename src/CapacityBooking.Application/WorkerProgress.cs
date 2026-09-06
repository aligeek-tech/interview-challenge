namespace CapacityBooking.Application;

/// <summary>Reports completed durable work, never a timer-only heartbeat.</summary>
public interface IWorkerProgress
{
    void ItemCompleted(string worker);
}

public sealed class HealthOptions
{
    public TimeSpan DatabaseTimeout { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan WorkerStaleAfter { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan BacklogWarningAge { get; set; } = TimeSpan.FromMinutes(2);
}

public sealed record DatabaseHealthSnapshot(bool Ready, string Code,
    long? PendingOutbox = null, double? OldestOutboxAgeSeconds = null,
    long? QuarantinedOutbox = null, long? DueHolds = null, double? OldestDueHoldAgeSeconds = null);
