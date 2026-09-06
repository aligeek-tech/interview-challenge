using CapacityBooking.Application;

/// <summary>Per-process worker state measured with monotonic time; business deadlines use PostgreSQL.</summary>
public sealed class WorkerHealthRegistry(TimeProvider clock) : IWorkerProgress
{
    private readonly object _gate = new();
    private readonly Dictionary<string, WorkerState> _workers = new(StringComparer.Ordinal);

    public void Started(string worker, bool enabled)
    {
        lock (_gate)
            _workers[worker] = new WorkerState { Enabled = enabled, LastActivity = clock.GetTimestamp() };
    }

    public void PollStarted(string worker)
    {
        lock (_gate)
            if (_workers.TryGetValue(worker, out var state)) state.Polling = true;
    }

    public void PollSucceeded(string worker)
    {
        lock (_gate)
        {
            if (!_workers.TryGetValue(worker, out var state)) return;
            state.LastActivity = clock.GetTimestamp();
            state.SuccessfulPolls++;
            state.ConsecutiveFailures = 0;
            state.LastError = null;
            state.Polling = false;
        }
    }

    public void PollFailed(string worker, Exception error)
    {
        lock (_gate)
        {
            if (!_workers.TryGetValue(worker, out var state)) return;
            state.ConsecutiveFailures++;
            state.LastError = error.GetType().Name;
            state.Polling = false;
            // A caught error must not refresh successful activity.
        }
    }

    public void ItemCompleted(string worker)
    {
        lock (_gate)
        {
            if (!_workers.TryGetValue(worker, out var state)) return;
            state.LastActivity = clock.GetTimestamp();
            state.CompletedItems++;
        }
    }

    public void Stopped(string worker)
    {
        lock (_gate)
            if (_workers.TryGetValue(worker, out var state)) state.Stopped = true;
    }

    public IReadOnlyList<WorkerHealthSnapshot> Snapshot(bool enabled, TimeSpan staleAfter)
    {
        lock (_gate)
        {
            return new[] { "expiry", "outbox" }.Select(name =>
            {
                if (!enabled) return new WorkerHealthSnapshot(name, "disabled", null, 0, 0, 0, null);
                if (!_workers.TryGetValue(name, out var state))
                    return new WorkerHealthSnapshot(name, "starting", null, 0, 0, 0, null);
                var age = clock.GetElapsedTime(state.LastActivity).TotalSeconds;
                var status = !state.Enabled ? "disabled" : state.Stopped ? "stopped" :
                    age > staleAfter.TotalSeconds ? "stalled" :
                    state.ConsecutiveFailures > 0 ? "failing" :
                    state.SuccessfulPolls == 0 && state.CompletedItems == 0 ? "starting" : "healthy";
                return new WorkerHealthSnapshot(name, status, age, state.SuccessfulPolls,
                    state.CompletedItems, state.ConsecutiveFailures, state.LastError, state.Polling);
            }).ToArray();
        }
    }

    private sealed class WorkerState
    {
        public bool Enabled { get; init; }
        public bool Polling { get; set; }
        public bool Stopped { get; set; }
        public long LastActivity { get; set; }
        public long SuccessfulPolls { get; set; }
        public long CompletedItems { get; set; }
        public long ConsecutiveFailures { get; set; }
        public string? LastError { get; set; }
    }
}

public sealed record WorkerHealthSnapshot(string Name, string Status, double? ActivityAgeSeconds,
    long SuccessfulPolls, long CompletedItems, long ConsecutiveFailures, string? LastError, bool InProgress = false);
