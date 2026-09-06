using System.Net;
using CapacityBooking.Application;
using Dapper;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class ConcurrencyTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Fact]
    public async Task TwentyIndependentDatabaseSessionsCompetingForOneUnitHaveExactlyOneWinner()
    {
        await Db.SeedAsync();
        await using var blocker = await Db.OpenAsync();
        await using var transaction = await blocker.BeginTransactionAsync();
        await blocker.ExecuteAsync("SELECT voyage_id FROM voyage_capacity WHERE voyage_id='voyage-1' FOR UPDATE", transaction: transaction);
        var requests = Enumerable.Range(0, 20).Select(index => Db.CreateAsync(
            booking: $"contender-{index}", key: $"request-{index}", client: index % 2 == 0 ? Db.Client : Db.SecondClient)).ToArray();
        try
        {
            // Twenty server sessions must simultaneously wait on database locks. This is stronger
            // evidence of overlap than Task.WhenAll or a synchronized client start alone.
            await Db.WaitForLockWaitersAsync(20);
        }
        finally { await transaction.CommitAsync(); }
        var responses = await Task.WhenAll(requests);
        try
        {
            Assert.Single(responses, response => response.StatusCode == HttpStatusCode.Created);
            Assert.Equal(19, responses.Count(response => response.StatusCode == HttpStatusCode.Conflict));
            Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds WHERE state='Active'"));
            await Db.AssertAccountingAsync(1, 0);
        }
        finally { foreach (var response in responses) response.Dispose(); }
    }

    [Fact]
    public async Task ConfirmationThatWaitsAcrossDeadlineUsesDatabaseTimeAfterTheLockWait()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        await SetDeadlineSoonAsync(hold.HoldId);
        await using var blocker = await Db.OpenAsync();
        await using var transaction = await blocker.BeginTransactionAsync();
        await blocker.ExecuteAsync("SELECT voyage_id FROM voyage_capacity WHERE voyage_id='voyage-1' FOR UPDATE", transaction: transaction);
        var confirmation = Db.ConfirmAsync(hold.HoldId);
        try
        {
            await Db.WaitForLockWaitersAsync(1);
            await WaitForDeadlineAsync(hold.HoldId);
        }
        finally { await transaction.CommitAsync(); }
        using var response = await confirmation;
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Equal("Expired", await Db.ScalarAsync<string>("SELECT state FROM capacity_holds"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
        await Db.AssertAccountingAsync(0, 0);
    }

    [Fact]
    public async Task ConfirmationDecisionBeforeDeadlineWinsEvenWhenExpiryWaitsAndCommitIsLater()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        await SetDeadlineSoonAsync(hold.HoldId);
        using var gate = Db.Observer.Gate("confirm.after-decision", hold.HoldId.ToString());
        var confirmation = Db.ConfirmAsync(hold.HoldId);
        await gate.WaitForEntryAsync();
        await WaitForDeadlineAsync(hold.HoldId);
        var expiry = Db.ExpireAsync(hold.HoldId);
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { gate.Release(); }
        using var response = await confirmation;
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.False(await expiry);
        Assert.Equal("Consumed", await Db.ScalarAsync<string>("SELECT state FROM capacity_holds"));
        await Db.AssertAccountingAsync(0, 1);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
    }

    [Fact]
    public async Task ExpiryHoldingTheBusinessLockWinsAgainstConcurrentConfirmation()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        await Db.ExecuteAsync("UPDATE capacity_holds SET created_at=clock_timestamp()-interval '2 minutes', expires_at=clock_timestamp()-interval '1 second' WHERE hold_id=@id", new { id = hold.HoldId });
        using var gate = Db.Observer.Gate("business.after-locks", hold.HoldId.ToString());
        var expiry = Db.ExpireAsync(hold.HoldId);
        await gate.WaitForEntryAsync();
        var confirmation = Db.ConfirmAsync(hold.HoldId);
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { gate.Release(); }
        Assert.True(await expiry);
        using var response = await confirmation;
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        await Db.AssertAccountingAsync(0, 0);
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
    }

    [Fact]
    public async Task ConcurrentDifferentKeysForOneBookingStillCreateOnlyOneHold()
    {
        await Db.SeedAsync(10);
        using var gate = Db.Observer.Gate("business.after-locks", "voyage-1");
        var firstTask = Db.CreateAsync(key: "one");
        await gate.WaitForEntryAsync();
        var secondTask = Db.CreateAsync(key: "two", client: Db.SecondClient);
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { gate.Release(); }
        using var first = await firstTask;
        using var second = await secondTask;
        Assert.Equal(HttpStatusCode.Created, first.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(1, 0);
    }

    [Fact]
    public async Task ConcurrentExpiryWorkersReleaseCapacityExactlyOnce()
    {
        await Db.SeedAsync(3);
        var hold = await Db.CreateHoldAsync(quantity: 3);
        await Db.ExecuteAsync("UPDATE capacity_holds SET created_at=clock_timestamp()-interval '2 minutes', expires_at=clock_timestamp()-interval '1 second'");
        using var gate = Db.Observer.Gate("business.after-locks", hold.HoldId.ToString());
        var first = Db.ExpireAsync(hold.HoldId);
        await gate.WaitForEntryAsync();
        var second = Db.ExpireAsync(hold.HoldId);
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { gate.Release(); }
        Assert.True(await first);
        Assert.False(await second);
        await Db.AssertAccountingAsync(0, 0);
    }

    private Task SetDeadlineSoonAsync(Guid hold) => Db.ExecuteAsync(
        "UPDATE capacity_holds SET created_at=clock_timestamp()-interval '1 minute', expires_at=clock_timestamp()+interval '1500 milliseconds' WHERE hold_id=@hold", new { hold });

    private Task WaitForDeadlineAsync(Guid hold) => DatabaseFixture.WaitUntilAsync(
        () => Db.ScalarAsync<bool>("SELECT clock_timestamp()>=expires_at FROM capacity_holds WHERE hold_id=@hold", new { hold }),
        "The database clock did not reach the configured hold expiry.");
}
