namespace CapacityBooking.Application;

/// <summary>
/// A worker-owned traversal cursor, never a claim on durable work. Keep this across
/// DI scopes; resetting it on restart safely rescans the persisted Active deadlines.
/// </summary>
public sealed class ExpirySweepState
{
    private int _inUse;
    public DateTimeOffset? Cutoff { get; private set; }
    public DateTimeOffset? LastExpiresAt { get; private set; }
    public Guid? LastHoldId { get; private set; }

    public IDisposable Enter()
    {
        if (Interlocked.CompareExchange(ref _inUse, 1, 0) != 0)
            throw new InvalidOperationException("Each expiry worker must own a separate sweep state.");
        return new Usage(this);
    }

    public void Begin(DateTimeOffset cutoff)
    {
        if (Cutoff.HasValue)
            throw new InvalidOperationException("The current expiry sweep has not completed.");
        Cutoff = cutoff;
    }

    public void Advance(DateTimeOffset expiresAt, Guid holdId)
    {
        LastExpiresAt = expiresAt;
        LastHoldId = holdId;
    }

    public void Complete()
    {
        Cutoff = null;
        LastExpiresAt = null;
        LastHoldId = null;
    }

    private sealed class Usage(ExpirySweepState state) : IDisposable
    {
        private ExpirySweepState? _state = state;
        public void Dispose()
        {
            var current = Interlocked.Exchange(ref _state, null);
            if (current is not null)
                Volatile.Write(ref current._inUse, 0);
        }
    }
}
