using CapacityBooking.Application;

namespace CapacityBooking.IntegrationTests;

/// <summary>In-process fault/gate seam: no public test endpoints or wall-clock ordering guesses.</summary>
public sealed class TestObserver : IExecutionObserver
{
    private Func<string, string, CancellationToken, Task> _callback = (_, _, _) => Task.CompletedTask;

    public void Configure(Func<string, string, CancellationToken, Task> callback) => _callback = callback;
    public void Reset() => _callback = (_, _, _) => Task.CompletedTask;
    public Task ReachedAsync(string point, string resourceId, CancellationToken ct = default) =>
        _callback(point, resourceId, ct);

    public ObserverGate Gate(string point, string? resource = null)
    {
        var gate = new ObserverGate();
        Configure(async (actualPoint, actualResource, ct) =>
        {
            if (actualPoint == point && (resource is null || actualResource == resource))
                await gate.EnterAsync(ct);
        });
        return gate;
    }

    public void FailAt(string point)
    {
        Configure((actualPoint, _, _) => actualPoint == point
            ? Task.FromException(new InjectedFailureException(point))
            : Task.CompletedTask);
    }
}

public sealed class ObserverGate : IDisposable
{
    private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task WaitForEntryAsync() => _entered.Task.WaitAsync(TimeSpan.FromSeconds(20));

    public async Task EnterAsync(CancellationToken ct)
    {
        _entered.TrySetResult();
        await _released.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);
    }

    public void Release() => _released.TrySetResult();
    public void Dispose() => Release();
}

public sealed class InjectedFailureException(string point) : Exception($"Injected test failure at {point}");
