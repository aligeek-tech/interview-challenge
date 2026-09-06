using CapacityBooking.Application;
using CapacityBooking.Infrastructure;
using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class ExpiryFairnessTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Fact]
    public async Task ColdVoyageBeyondTheOriginalHundredAndAnEntirePollBudgetExpiresBeforeHotLockIsReleased()
    {
        await SeedDueAsync("hot", 129, 600);
        var cold = (await SeedDueAsync("cold", 1, 300))[0];
        var state = new ExpirySweepState();
        var options = BoundedOptions(page: 4, candidates: 8);
        await using var blocker = await Db.OpenAsync();
        await using var blocked = await blocker.BeginTransactionAsync();
        await blocker.ExecuteAsync("SELECT voyage_id FROM voyage_capacity WHERE voyage_id='hot' FOR UPDATE", transaction: blocked);
        try
        {
            var totalExpired = 0;
            // Recreate the scoped service each tick, retaining only the worker's traversal state.
            for (var poll = 0; poll < 17; poll++)
            {
                var result = await Service(options).SweepDueAsync(state).WaitAsync(TimeSpan.FromSeconds(5));
                Assert.InRange(result.Examined, 0, 8);
                Assert.False(result.BudgetExhausted);
                totalExpired += result.Expired;
            }
            Assert.Equal(1, totalExpired);
            Assert.Equal("Expired", await StateAsync(cold));
            Assert.Equal(129, await Db.ScalarAsync<int>("SELECT count(*)::integer FROM capacity_holds WHERE voyage_id='hot' AND state='Active'"));
            await Db.AssertAccountingAsync(129, 0, "hot");
            await Db.AssertAccountingAsync(0, 0, "cold");
            Assert.Equal(1, await ExpiryAuditsAsync(cold));
        }
        finally { await blocked.RollbackAsync(); }
    }

    [Fact]
    public async Task TwoWorkersUseIndependentSessionsAndColdWorkProgressesWhileTheOtherWorkerIsPaused()
    {
        await SeedDueAsync("hot", 1, 600);
        var firstCold = (await SeedDueAsync("cold-a", 1, 400))[0];
        var secondCold = (await SeedDueAsync("cold-b", 1, 300))[0];
        var firstObserver = new TestObserver();
        using var gate = firstObserver.Gate("business.after-locks", firstCold.ToString("D"));
        var firstProgress = new ProgressRecorder();
        var secondProgress = new ProgressRecorder();
        var firstName = "expiry-worker-a-" + Guid.NewGuid().ToString("N");
        var secondName = "expiry-worker-b-" + Guid.NewGuid().ToString("N");
        var options = BoundedOptions(page: 8, candidates: 8);
        options.PollTimeBudget = TimeSpan.FromSeconds(10);
        var firstService = Service(options, firstObserver, firstProgress, firstName);
        var secondService = Service(options, progress: secondProgress, applicationName: secondName);
        await using var blocker = await Db.OpenAsync();
        await using var blocked = await blocker.BeginTransactionAsync();
        await blocker.ExecuteAsync("SELECT voyage_id FROM voyage_capacity WHERE voyage_id='hot' FOR UPDATE", transaction: blocked);
        var first = firstService.SweepDueAsync(new ExpirySweepState());
        try
        {
            await gate.WaitForEntryAsync();
            var second = await secondService.SweepDueAsync(new ExpirySweepState()).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(1, second.Expired);
            Assert.Equal("Expired", await StateAsync(secondCold));
            Assert.False(first.IsCompleted);
            // MaxPoolSize=1 for each named worker; this proves two independent server sessions.
            Assert.Equal(2, await Db.ScalarAsync<int>("""
                SELECT count(DISTINCT pid)::integer FROM pg_stat_activity
                WHERE datname=current_database() AND application_name = ANY(@Names)
                """, new { Names = new[] { firstName, secondName } }));
            gate.Release();
            var firstResult = await first.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(1, firstResult.Expired);
            Assert.Equal("Expired", await StateAsync(firstCold));
            Assert.Equal(1, firstProgress.Completed);
            Assert.Equal(1, secondProgress.Completed);
            Assert.Equal(1, await ExpiryAuditsAsync(firstCold));
            Assert.Equal(1, await ExpiryAuditsAsync(secondCold));
            await Db.AssertAccountingAsync(1, 0, "hot");
        }
        finally
        {
            gate.Release();
            await first;
            await blocked.RollbackAsync();
        }
    }

    [Theory]
    [InlineData("booking")]
    [InlineData("hold")]
    public async Task TailRowLockIsBoundedAndDoesNotDelayAnotherVoyage(string target)
    {
        var hot = (await SeedDueAsync("hot", 1, 600))[0];
        var cold = (await SeedDueAsync("cold", 1, 300))[0];
        await using var blocker = await Db.OpenAsync();
        await using var blocked = await blocker.BeginTransactionAsync();
        var sql = target == "booking"
            ? "SELECT booking_id FROM bookings WHERE voyage_id='hot' FOR UPDATE"
            : "SELECT hold_id FROM capacity_holds WHERE voyage_id='hot' FOR UPDATE";
        await blocker.ExecuteAsync(sql, transaction: blocked);
        try
        {
            var result = await Service(BoundedOptions()).SweepDueAsync(new ExpirySweepState()).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(1, result.Busy);
            Assert.Equal(1, result.Expired);
            Assert.False(result.BudgetExhausted);
            Assert.Equal("Active", await StateAsync(hot));
            Assert.Equal("Expired", await StateAsync(cold));
            await Db.AssertAccountingAsync(1, 0, "hot");
            await Db.AssertAccountingAsync(0, 0, "cold");
            // The aborted busy-item transaction released its voyage lock before returning.
            await using var proof = await Db.OpenAsync();
            await using var proofTransaction = await proof.BeginTransactionAsync();
            await proof.ExecuteAsync("SELECT voyage_id FROM voyage_capacity WHERE voyage_id='hot' FOR UPDATE NOWAIT", transaction: proofTransaction);
        }
        finally { await blocked.RollbackAsync(); }
    }

    [Fact]
    public async Task BusyHoldDoesNotHideOtherHoldsOnItsOtherwiseAvailableVoyage()
    {
        var holds = await SeedDueAsync("voyage-1", 2, 300);
        await using var blocker = await Db.OpenAsync();
        await using var blocked = await blocker.BeginTransactionAsync();
        await blocker.ExecuteAsync("SELECT hold_id FROM capacity_holds WHERE hold_id=@id FOR UPDATE",
            new { id = holds[0] }, blocked);
        var result = await Service(BoundedOptions()).SweepDueAsync(new ExpirySweepState()).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, result.Busy);
        Assert.Equal(1, result.Expired);
        Assert.Equal("Active", await StateAsync(holds[0]));
        Assert.Equal("Expired", await StateAsync(holds[1]));
        await Db.AssertAccountingAsync(1, 0);
    }

    [Fact]
    public async Task PollDeadlineRollsBackTheInterruptedItemAndLeavesItDiscoverable()
    {
        var hold = (await SeedDueAsync("voyage-1", 1, 300))[0];
        var observer = new TestObserver();
        using var gate = observer.Gate("expiry.before-commit", hold.ToString("D"));
        var options = BoundedOptions();
        options.PollTimeBudget = TimeSpan.FromSeconds(1);
        var state = new ExpirySweepState();
        var progress = new ProgressRecorder();
        var worker = Service(options, observer, progress);
        var pending = worker.SweepDueAsync(state);
        await gate.WaitForEntryAsync();
        var result = await pending.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(result.BudgetExhausted);
        Assert.Equal(0, result.ResolvedCandidates);
        Assert.Equal(0, result.Expired);
        Assert.Equal(0, progress.Completed);
        Assert.Null(state.LastHoldId);
        Assert.Equal("Active", await StateAsync(hold));
        Assert.Equal(0, await ExpiryAuditsAsync(hold));
        await Db.AssertAccountingAsync(1, 0);
        observer.Reset();
        Assert.Equal(1, (await worker.SweepDueAsync(state)).Expired);
        Assert.Equal(1, progress.Completed);
        Assert.Equal(1, await ExpiryAuditsAsync(hold));
    }

    [Fact]
    public async Task ExternalCancellationReleasesTheTransactionAndFreshWorkerCanRecover()
    {
        var hold = (await SeedDueAsync("voyage-1", 1, 300))[0];
        var observer = new TestObserver();
        using var gate = observer.Gate("expiry.before-commit", hold.ToString("D"));
        using var stop = new CancellationTokenSource();
        var state = new ExpirySweepState();
        var pending = Service(BoundedOptions(), observer).SweepDueAsync(state, ct: stop.Token);
        await gate.WaitForEntryAsync();
        stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        await Db.AssertAccountingAsync(1, 0);
        Assert.Equal(0, await ExpiryAuditsAsync(hold));
        Assert.Equal(1, (await Service(BoundedOptions()).SweepDueAsync(new ExpirySweepState())).Expired);
        await Db.AssertAccountingAsync(0, 0);
    }

    [Fact]
    public async Task RestartedSweepRevisitsPreviouslySkippedAndUnprocessedDeadlines()
    {
        await SeedDueAsync("hot", 3, 600);
        await SeedDueAsync("cold", 2, 300);
        var options = BoundedOptions(page: 2, candidates: 3);
        await using (var blocker = await Db.OpenAsync())
        await using (var blocked = await blocker.BeginTransactionAsync())
        {
            await blocker.ExecuteAsync("SELECT voyage_id FROM voyage_capacity WHERE voyage_id='hot' FOR UPDATE", transaction: blocked);
            var first = await Service(options).SweepDueAsync(new ExpirySweepState());
            Assert.Equal(3, first.Busy);
            Assert.Equal(0, first.Expired);
        }
        var restartedState = new ExpirySweepState();
        var expired = 0;
        for (var poll = 0; poll < 2; poll++)
            expired += (await Service(options).SweepDueAsync(restartedState)).Expired;
        Assert.Equal(5, expired);
        Assert.Equal(5, await Db.ScalarAsync<int>("SELECT count(*)::integer FROM audit_transitions WHERE transition='CapacityHoldExpired'"));
        await Db.AssertAccountingAsync(0, 0, "hot");
        await Db.AssertAccountingAsync(0, 0, "cold");
    }

    [Fact]
    public async Task BusyOnlyPollIsBoundedAndDoesNotReportProductiveProgress()
    {
        await SeedDueAsync("hot", 20, 600);
        var options = BoundedOptions(page: 2, candidates: 3);
        var progress = new ProgressRecorder();
        await using var blocker = await Db.OpenAsync();
        await using var blocked = await blocker.BeginTransactionAsync();
        await blocker.ExecuteAsync("SELECT voyage_id FROM voyage_capacity WHERE voyage_id='hot' FOR UPDATE", transaction: blocked);
        var result = await Service(options, progress: progress).SweepDueAsync(new ExpirySweepState()).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(3, result.Examined);
        Assert.Equal(3, result.ResolvedCandidates);
        Assert.Equal(3, result.Busy);
        Assert.Equal(0, result.Expired);
        Assert.Equal(0, progress.Completed);
        Assert.False(result.SweepCompleted);
        Assert.False(result.BudgetExhausted);
    }

    private ExpiryService Service(ExpiryOptions options, IExecutionObserver? observer = null,
        IWorkerProgress? progress = null, string? applicationName = null)
    {
        var connection = new NpgsqlConnectionStringBuilder(Db.ConnectionString)
        {
            ApplicationName = applicationName ?? "expiry-fairness",
            MaxPoolSize = 1
        };
        return new ExpiryService(new Database(connection.ConnectionString), observer ?? new NullExecutionObserver(),
            NullLogger<ExpiryService>.Instance, Options.Create(options), progress);
    }

    private static ExpiryOptions BoundedOptions(int page = 8, int candidates = 16) => new()
    {
        PageSize = page,
        MaxCandidatesPerPoll = candidates,
        PollTimeBudget = TimeSpan.FromSeconds(3),
        LockWaitTimeout = TimeSpan.FromMilliseconds(50)
    };

    private async Task<Guid[]> SeedDueAsync(string voyage, int count, int secondsAgo)
    {
        var ids = Enumerable.Range(0, count).Select(_ => Guid.NewGuid()).ToArray();
        // Direct setup creates coherent domain state; these tests exercise scheduling/expiry,
        // while creation behavior is independently covered by the original suite.
        await Db.ExecuteAsync("""
            INSERT INTO voyage_capacity (voyage_id,total,reserved,confirmed,is_open)
            VALUES (@Voyage,@Count,@Count,0,true);
            INSERT INTO bookings (booking_id,voyage_id,customer_id,quantity)
            SELECT @Voyage || '-booking-' || n::text, @Voyage, 'customer-1', 1
            FROM generate_series(1,@Count) n;
            INSERT INTO capacity_holds
                (hold_id,booking_id,voyage_id,quantity,state,created_at,expires_at)
            SELECT ids.hold_id, @Voyage || '-booking-' || ids.n::text, @Voyage, 1, 'Active',
                   statement_timestamp() - (@SecondsAgo + 60) * interval '1 second',
                   statement_timestamp() - @SecondsAgo * interval '1 second' + ids.n * interval '1 millisecond'
            FROM unnest(@Ids) WITH ORDINALITY ids(hold_id,n)
            """, new { Voyage = voyage, Count = count, SecondsAgo = secondsAgo, Ids = ids });
        return ids;
    }

    private Task<string> StateAsync(Guid id) => Db.ScalarAsync<string>("SELECT state FROM capacity_holds WHERE hold_id=@id", new { id });
    private Task<int> ExpiryAuditsAsync(Guid id) => Db.ScalarAsync<int>(
        "SELECT count(*)::integer FROM audit_transitions WHERE hold_id=@id AND transition='CapacityHoldExpired'", new { id });

    private sealed class ProgressRecorder : IWorkerProgress
    {
        private int _completed;
        public int Completed => Volatile.Read(ref _completed);
        public void ItemCompleted(string worker)
        {
            Assert.Equal("expiry", worker);
            Interlocked.Increment(ref _completed);
        }
    }
}
