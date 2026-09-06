using System.Data;
using System.Diagnostics;
using CapacityBooking.Application;
using CapacityBooking.Infrastructure.Business;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using ExpiryCandidate = CapacityBooking.Infrastructure.Business.BookingPersistenceSession.ExpiryCandidate;

namespace CapacityBooking.Infrastructure;

/// <summary>Bounded background traversal plus the shared, transactionally persisted domain expiry transition.</summary>
public sealed class ExpiryService : IExpiryService
{
    private readonly Database _database;
    private readonly IExecutionObserver _observer;
    private readonly ILogger<ExpiryService> _logger;
    private readonly ExpiryOptions _options;
    private readonly IWorkerProgress? _progress;
    private readonly ExpirySweepState _localSweep = new();

    public ExpiryService(Database database, IExecutionObserver observer, ILogger<ExpiryService> logger,
        IOptions<ExpiryOptions>? options = null, IWorkerProgress? progress = null)
    {
        _database = database;
        _observer = observer;
        _logger = logger;
        _options = options?.Value ?? new ExpiryOptions();
        _progress = progress;
        if (_options.PageSize is < 1 or > 1024 || _options.MaxCandidatesPerPoll is < 1 or > 10_000 ||
            _options.PollTimeBudget <= TimeSpan.Zero || _options.PollTimeBudget > TimeSpan.FromMinutes(1) ||
            _options.LockWaitTimeout <= TimeSpan.Zero || _options.LockWaitTimeout > _options.PollTimeBudget)
            throw new ArgumentOutOfRangeException(nameof(options), "Expiry page, candidate, deadline and lock-wait bounds must be valid.");
    }

    public async Task<int> ExpireDueAsync(int batchSize = 100, CancellationToken ct = default) =>
        (await SweepDueAsync(_localSweep, batchSize, ct)).Expired;

    public async Task<ExpirySweepResult> SweepDueAsync(ExpirySweepState state, int batchSize = 100,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        using var usage = state.Enter();
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct);
        budget.CancelAfter(_options.PollTimeBudget);
        var workCt = budget.Token;
        var expired = 0;
        var examined = 0;
        var resolved = 0;
        var busy = 0;
        var sweepCompleted = false;
        var budgetExhausted = false;
        // Bounded by MaxCandidatesPerPoll. One hot voyage costs only one lock attempt per poll.
        var busyVoyages = new HashSet<string>(StringComparer.Ordinal);
        try
        {
            if (!state.Cutoff.HasValue)
            {
                await using var connection = await _database.OpenAsync(workCt);
                state.Begin(await new BookingPersistenceSession(connection).ReadClockAsync(workCt));
            }

            while (examined < _options.MaxCandidatesPerPoll && expired < batchSize)
            {
                workCt.ThrowIfCancellationRequested();
                var pageSize = Math.Min(_options.PageSize, _options.MaxCandidatesPerPoll - examined);
                IReadOnlyList<ExpiryCandidate> page;
                await using (var connection = await _database.OpenAsync(workCt))
                    page = await new BookingPersistenceSession(connection).ReadDuePageAsync(state, pageSize, workCt);
                if (page.Count == 0)
                {
                    state.Complete();
                    sweepCompleted = true;
                    break;
                }

                foreach (var candidate in page)
                {
                    if (expired >= batchSize)
                        break;
                    workCt.ThrowIfCancellationRequested();
                    examined++;
                    var outcome = busyVoyages.Contains(candidate.VoyageId)
                        ? ExpiryAttempt.BusyVoyage
                        : await ExpireCandidateAsync(candidate, skipBusy: true, workCt);
                    if (outcome == ExpiryAttempt.Expired)
                        expired++;
                    else if (outcome is ExpiryAttempt.BusyVoyage or ExpiryAttempt.BusyItem)
                    {
                        busy++;
                        // A locked booking/hold does not make every other hold on this
                        // voyage unavailable once its failed transaction has rolled back.
                        if (outcome == ExpiryAttempt.BusyVoyage)
                            busyVoyages.Add(candidate.VoyageId);
                    }
                    // Advance only after the attempt resolved. Cancellation leaves the interrupted
                    // item discoverable from the prior cursor; skipped work is revisited next sweep.
                    state.Advance(candidate.ExpiresAt, candidate.HoldId);
                    resolved++;
                }
                if (expired < batchSize && page.Count < pageSize)
                {
                    state.Complete();
                    sweepCompleted = true;
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested && budget.IsCancellationRequested)
        {
            budgetExhausted = true;
        }
        ct.ThrowIfCancellationRequested();
        _logger.LogInformation(
            "Expiry sweep poll completed: Expired {Expired}, Examined {Examined}, Busy {Busy}, SweepCompleted {SweepCompleted}, BudgetExhausted {BudgetExhausted}",
            expired, examined, busy, sweepCompleted, budgetExhausted);
        return new ExpirySweepResult(expired, examined, busy, sweepCompleted, budgetExhausted, resolved);
    }

    /// <summary>Explicit expiry retains blocking semantics for deterministic command/race behavior.</summary>
    public async Task<bool> ExpireAsync(Guid holdId, CancellationToken ct = default)
    {
        BookingPersistenceSession.HoldIdentity? identity;
        await using (var connection = await _database.OpenAsync(ct))
            identity = await new BookingPersistenceSession(connection).FindHoldIdentityAsync(holdId, ct);
        if (identity is null)
            return false;
        var candidate = new ExpiryCandidate(holdId, identity.BookingId, identity.VoyageId, default);
        return await ExpireCandidateAsync(candidate, skipBusy: false, ct) == ExpiryAttempt.Expired;
    }

    private async Task<ExpiryAttempt> ExpireCandidateAsync(ExpiryCandidate candidate, bool skipBusy, CancellationToken ct)
    {
        await using var connection = await _database.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var session = new BookingPersistenceSession(connection, transaction);
        try
        {
            if (skipBusy)
                await session.SetLocalLockTimeoutAsync(_options.LockWaitTimeout, ct);
            // SKIP LOCKED applies to the root, never a candidate hold. The local timeout
            // also bounds later booking/hold locks and relation locks; the poll token bounds I/O.
            var capacity = await session.LockCapacityAsync(candidate.VoyageId, ct, skipLocked: skipBusy);
            if (capacity is null)
                return skipBusy ? ExpiryAttempt.BusyVoyage : ExpiryAttempt.NoChange;
            var booking = await session.LockBookingAsync(candidate.BookingId, ct);
            var hold = await session.LockHoldAsync(candidate.HoldId, ct);
            if (booking is null || hold is null)
                return ExpiryAttempt.NoChange;

            await _observer.ReachedAsync("business.after-locks", candidate.HoldId.ToString("D"), ct);
            var decisionTime = await session.ReadClockAsync(ct);
            var context = new RequestContext(booking.CustomerId, Activity.Current?.TraceId.ToString() ?? "expiry-worker");
            if (!await HoldExpiryTransition.ApplyAsync(session, capacity, hold, decisionTime, context,
                    "Capacity returned after the hold deadline.", ct))
            {
                await transaction.CommitAsync(ct);
                _logger.LogInformation(
                    "Expiry made no transition for HoldId {HoldId}, BookingId {BookingId}, VoyageId {VoyageId}: State {State}, DecisionTime {DecisionTime}, ExpiresAt {ExpiresAt}",
                    hold.HoldId, hold.BookingId, hold.VoyageId, hold.State, decisionTime, hold.Deadline.ExpiresAt);
                return ExpiryAttempt.NoChange;
            }
            await _observer.ReachedAsync("expiry.before-commit", candidate.HoldId.ToString("D"), ct);
            await transaction.CommitAsync(ct);
            if (skipBusy)
                _progress?.ItemCompleted("expiry");
            _logger.LogInformation(
                "CapacityHoldExpired: HoldId {HoldId}, BookingId {BookingId}, VoyageId {VoyageId}, Quantity {Quantity}, DecisionTime {DecisionTime}, TraceId {TraceId}",
                hold.HoldId, hold.BookingId, hold.VoyageId, hold.Quantity.Value, decisionTime, context.TraceId);
            return ExpiryAttempt.Expired;
        }
        catch (PostgresException error) when (skipBusy && error.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            // The failed transaction is disposed/rolled back before another candidate is tried.
            _logger.LogDebug("Expiry deferred a busy item: HoldId {HoldId}, VoyageId {VoyageId}", candidate.HoldId, candidate.VoyageId);
            return ExpiryAttempt.BusyItem;
        }
    }

    private enum ExpiryAttempt { NoChange, Expired, BusyVoyage, BusyItem }
}
