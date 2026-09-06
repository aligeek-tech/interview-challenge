using System.Net;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class ConcurrentCommandTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ConcurrentConfirmationsAcrossHostsProduceOneConfirmationAndOneEvent(bool sameKey)
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        using var gate = Db.Observer.Gate("confirm.before-commit", hold.HoldId.ToString());
        var firstTask = Db.ConfirmAsync(hold.HoldId, "confirm-first");
        await gate.WaitForEntryAsync();
        var secondTask = Db.ConfirmAsync(hold.HoldId, sameKey ? "confirm-first" : "confirm-second", client: Db.SecondClient);
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { gate.Release(); }

        using var first = await firstTask;
        using var second = await secondTask;
        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
        Assert.Equal(await first.Content.ReadAsStringAsync(), await second.Content.ReadAsStringAsync());
        Assert.Equal(sameKey, second.Headers.Contains("Idempotency-Replayed"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM bookings WHERE confirmed_hold_id IS NOT NULL"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM audit_transitions WHERE transition='BookingConfirmed'"));
        Assert.Equal(sameKey ? 2 : 3, await Db.ScalarAsync<int>("SELECT count(*)::int FROM idempotency_records"));
        await Db.AssertAccountingAsync(0, 1);
    }

    [Fact]
    public async Task ConcurrentSameKeyWithDifferentPayloadWaitsThenRejectsWithoutChangingTheOriginalResponse()
    {
        await Db.SeedAsync(10);
        using var gate = Db.Observer.Gate("business.after-locks", "voyage-1");
        var originalTask = Db.CreateAsync(quantity: 1, key: "shared-key");
        await gate.WaitForEntryAsync();
        var conflictingTask = Db.CreateAsync(quantity: 2, key: "shared-key", client: Db.SecondClient);
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { gate.Release(); }

        using var original = await originalTask;
        using var conflict = await conflictingTask;
        Assert.Equal(HttpStatusCode.Created, original.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
        Assert.False(conflict.Headers.Contains("Idempotency-Replayed"));
        using var replay = await Db.CreateAsync(quantity: 1, key: "shared-key", client: Db.SecondClient);
        Assert.Equal(HttpStatusCode.Created, replay.StatusCode);
        Assert.Equal(await original.Content.ReadAsStringAsync(), await replay.Content.ReadAsStringAsync());
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM idempotency_records"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(1, 0);
    }

    [Fact]
    public async Task WaitingDuplicateTakesOverAfterTheOriginalConfirmationOwnerRollsBack()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        using var originalOwner = new ObserverGate();
        var reachedCommit = 0;
        Db.Observer.Configure(async (point, resource, ct) =>
        {
            if (point == "confirm.before-commit" && resource == hold.HoldId.ToString() &&
                Interlocked.Increment(ref reachedCommit) == 1)
            {
                await originalOwner.EnterAsync(ct);
                throw new InjectedFailureException(point);
            }
        });
        var originalTask = Db.ConfirmAsync(hold.HoldId, "retry-owner-key");
        await originalOwner.WaitForEntryAsync();
        var waitingTask = Db.ConfirmAsync(hold.HoldId, "retry-owner-key", client: Db.SecondClient);
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { originalOwner.Release(); }

        using var failedOwner = await originalTask;
        using var successfulWaiter = await waitingTask;
        Assert.Equal(HttpStatusCode.InternalServerError, failedOwner.StatusCode);
        Assert.Equal(HttpStatusCode.OK, successfulWaiter.StatusCode);
        Assert.False(successfulWaiter.Headers.Contains("Idempotency-Replayed"));
        Assert.Equal(2, reachedCommit);
        using var replay = await Db.ConfirmAsync(hold.HoldId, "retry-owner-key");
        Assert.Equal(await successfulWaiter.Content.ReadAsStringAsync(), await replay.Content.ReadAsStringAsync());
        Assert.True(replay.Headers.Contains("Idempotency-Replayed"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM audit_transitions WHERE transition='BookingConfirmed'"));
        Assert.Equal(2, await Db.ScalarAsync<int>("SELECT count(*)::int FROM idempotency_records"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM idempotency_records WHERE status_code IS NULL OR response_json IS NULL"));
        await Db.AssertAccountingAsync(0, 1);
    }

    [Fact]
    public async Task ConcurrentRejectedCommandsReplayTheCommittedRejectionWithoutRepeatingBusinessWork()
    {
        await Db.SeedAsync(10);
        var existing = await Db.CreateHoldAsync();
        using var gate = Db.Observer.Gate("business.after-locks", "voyage-1");
        var firstTask = Db.CreateAsync(key: "rejected-key");
        await gate.WaitForEntryAsync();
        var secondTask = Db.CreateAsync(key: "rejected-key", client: Db.SecondClient);
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { gate.Release(); }

        using var first = await firstTask;
        using var second = await secondTask;
        Assert.Equal(HttpStatusCode.Conflict, first.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
        Assert.Equal(await first.Content.ReadAsStringAsync(), await second.Content.ReadAsStringAsync());
        Assert.True(second.Headers.Contains("Idempotency-Replayed"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        Assert.Equal(existing.HoldId, await Db.ScalarAsync<Guid>("SELECT hold_id FROM capacity_holds"));
        Assert.Equal(existing.ExpiresAt.UtcDateTime, await Db.ScalarAsync<DateTime>("SELECT expires_at FROM capacity_holds"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM audit_transitions WHERE transition='CapacityHoldRejected'"));
        Assert.Equal(2, await Db.ScalarAsync<int>("SELECT count(*)::int FROM idempotency_records"));
        await Db.AssertAccountingAsync(1, 0);
    }
}
