namespace CapacityBooking.Application;

public sealed class ExpiryOptions
{
    public int PageSize { get; set; } = 64;
    public int MaxCandidatesPerPoll { get; set; } = 256;
    public TimeSpan PollTimeBudget { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan LockWaitTimeout { get; set; } = TimeSpan.FromMilliseconds(50);
}

public sealed record ExpirySweepResult(int Expired, int Examined, int Busy,
    bool SweepCompleted, bool BudgetExhausted, int ResolvedCandidates = 0);
