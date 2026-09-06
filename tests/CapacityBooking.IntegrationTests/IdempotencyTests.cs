using System.Net;
using System.Net.Http.Json;
using CapacityBooking.Application;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class IdempotencyTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Fact]
    public async Task DuplicateCreateAcrossTwoHostsReturnsOriginalResponseAndOriginalDeadline()
    {
        await Db.SeedAsync(5);
        using var first = await Db.CreateAsync();
        var originalJson = await first.Content.ReadAsStringAsync();
        var original = (await first.Content.ReadFromJsonAsync<HoldView>())!;
        using var second = await Db.CreateAsync(client: Db.SecondClient);
        Assert.Equal(HttpStatusCode.Created, second.StatusCode);
        Assert.Equal(originalJson, await second.Content.ReadAsStringAsync());
        Assert.True(second.Headers.TryGetValues("Idempotency-Replayed", out var replay));
        Assert.Equal("true", Assert.Single(replay!), ignoreCase: true);
        Assert.Equal(original.ExpiresAt, (await second.Content.ReadFromJsonAsync<HoldView>())!.ExpiresAt);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM idempotency_records"));
        await Db.AssertAccountingAsync(1, 0);
    }

    [Fact]
    public async Task ConcurrentDuplicateCreatesWaitOnThePersistedClaimAcrossHosts()
    {
        await Db.SeedAsync(5);
        using var gate = Db.Observer.Gate("business.after-locks", "voyage-1");
        var firstTask = Db.CreateAsync();
        await gate.WaitForEntryAsync();
        var secondTask = Db.CreateAsync(client: Db.SecondClient);
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { gate.Release(); }
        using var first = await firstTask;
        using var second = await secondTask;
        Assert.Equal(HttpStatusCode.Created, first.StatusCode);
        Assert.Equal(first.StatusCode, second.StatusCode);
        Assert.Equal(await first.Content.ReadAsStringAsync(), await second.Content.ReadAsStringAsync());
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(1, 0);
    }

    [Fact]
    public async Task SameKeyWithDifferentPayloadIsAConflict()
    {
        await Db.SeedAsync(10);
        await Db.CreateHoldAsync();
        using var changedQuantity = await Db.CreateAsync(quantity: 2);
        using var changedBooking = await Db.CreateAsync(booking: "different-booking");
        Assert.Equal(HttpStatusCode.Conflict, changedQuantity.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, changedBooking.StatusCode);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(1, 0);
    }

    [Fact]
    public async Task ConfirmationReplayIsStableAcrossHostsAndProducesOneOutboxEvent()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        using var first = await Db.ConfirmAsync(hold.HoldId);
        using var second = await Db.ConfirmAsync(hold.HoldId, client: Db.SecondClient);
        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
        Assert.Equal(await first.Content.ReadAsStringAsync(), await second.Content.ReadAsStringAsync());
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM bookings WHERE confirmed_hold_id IS NOT NULL"));
        await Db.AssertAccountingAsync(0, 1);
    }

    [Fact]
    public async Task ExpectedRejectionReplaysAfterCapacityBecomesAvailable()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        using var firstRejection = await Db.CreateAsync(booking: "booking-2", key: "rejected-request");
        Assert.Equal(HttpStatusCode.Conflict, firstRejection.StatusCode);
        using var cancelled = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Delete);
        using var replay = await Db.CreateAsync(booking: "booking-2", key: "rejected-request", client: Db.SecondClient);
        Assert.Equal(firstRejection.StatusCode, replay.StatusCode);
        Assert.Equal(await firstRejection.Content.ReadAsStringAsync(), await replay.Content.ReadAsStringAsync());
        using var newAttempt = await Db.CreateAsync(booking: "booking-2", key: "new-logical-attempt");
        Assert.Equal(HttpStatusCode.Created, newAttempt.StatusCode);
        await Db.AssertAccountingAsync(1, 0);
    }

    [Fact]
    public async Task ReplayingCreateAfterCancellationDoesNotCreateAReplacementOrExtendTtl()
    {
        await Db.SeedAsync();
        using var original = await Db.CreateAsync();
        var hold = (await original.Content.ReadFromJsonAsync<HoldView>())!;
        using var cancelled = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Delete);
        using var replay = await Db.CreateAsync(client: Db.SecondClient);
        Assert.Equal(await original.Content.ReadAsStringAsync(), await replay.Content.ReadAsStringAsync());
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(0, 0);
    }

    [Fact]
    public async Task MutatingPostEndpointsRequireIdempotencyKeys()
    {
        await Db.SeedAsync();
        using var missingCreateKey = await Db.CreateAsync(key: null);
        Assert.Equal(HttpStatusCode.BadRequest, missingCreateKey.StatusCode);
        var hold = await Db.CreateHoldAsync();
        using var missingConfirmKey = await Db.ConfirmAsync(hold.HoldId, key: null);
        Assert.Equal(HttpStatusCode.BadRequest, missingConfirmKey.StatusCode);
        await Db.AssertAccountingAsync(1, 0);
    }
}
