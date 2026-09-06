using System.Net;
using System.Net.Http.Json;
using CapacityBooking.Application;
using Npgsql;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class BusinessLifecycleTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Fact]
    public async Task ClosedVoyageRejectsHoldWithoutReservingCapacity()
    {
        await Db.SeedAsync(open: false);
        using var response = await Db.CreateAsync();
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(0, 0);
    }

    [Fact]
    public async Task CreateReadConfirmMovesReservedUnitsExactlyOnceAndPersistsEvent()
    {
        await Db.SeedAsync(5);
        var hold = await Db.CreateHoldAsync(quantity: 3);
        Assert.Equal("Active", hold.State);
        Assert.True(hold.ExpiresAt > hold.CreatedAt);
        await Db.AssertAccountingAsync(3, 0);
        using var read = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Get);
        Assert.Equal(hold, await read.Content.ReadFromJsonAsync<HoldView>());
        using var confirmed = await Db.ConfirmAsync(hold.HoldId);
        Assert.Equal(HttpStatusCode.OK, confirmed.StatusCode);
        var booking = await confirmed.Content.ReadFromJsonAsync<ConfirmationView>();
        Assert.Equal(hold.HoldId, booking!.HoldId);
        Assert.Equal(3, booking.Quantity);
        await Db.AssertAccountingAsync(0, 3);
        Assert.Equal("Consumed", await Db.ScalarAsync<string>("SELECT state FROM capacity_holds"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE event_type='BookingConfirmed'"));
        Assert.True(await Db.ScalarAsync<int>("SELECT count(*)::int FROM audit_transitions") >= 2);
    }

    [Fact]
    public async Task CancelIsRepeatableAndCancelledHoldCannotConfirm()
    {
        await Db.SeedAsync(2);
        var hold = await Db.CreateHoldAsync(quantity: 2);
        using var first = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Delete);
        using var second = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Delete);
        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.Equal(await first.Content.ReadAsStringAsync(), await second.Content.ReadAsStringAsync());
        Assert.Equal("Cancelled", (await second.Content.ReadFromJsonAsync<HoldView>())!.State);
        Assert.False(await Db.ExpireAsync(hold.HoldId));
        using var confirm = await Db.ConfirmAsync(hold.HoldId);
        Assert.Equal(HttpStatusCode.Conflict, confirm.StatusCode);
        await Db.AssertAccountingAsync(0, 0);
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
    }

    [Fact]
    public async Task ExpiredHoldCannotConfirmAndExpiryNeverReleasesTwice()
    {
        await Db.SeedAsync(2);
        var hold = await Db.CreateHoldAsync(quantity: 2);
        await MakeExpiredAsync(hold.HoldId);
        using var confirm = await Db.ConfirmAsync(hold.HoldId);
        Assert.Equal(HttpStatusCode.Conflict, confirm.StatusCode);
        await Db.ExpireDueAsync();
        Assert.False(await Db.ExpireAsync(hold.HoldId));
        using var cancel = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Delete);
        Assert.Equal(HttpStatusCode.OK, cancel.StatusCode);
        await Db.AssertAccountingAsync(0, 0);
        Assert.Equal("Expired", await Db.ScalarAsync<string>("SELECT state FROM capacity_holds"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
    }

    [Fact]
    public async Task ReadProjectsAnElapsedDeadlineWithoutMutatingTheStoredHold()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        await MakeExpiredAsync(hold.HoldId);
        using var response = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Get);
        Assert.Equal("Expired", (await response.Content.ReadFromJsonAsync<HoldView>())!.State);
        Assert.Equal("Active", await Db.ScalarAsync<string>("SELECT state FROM capacity_holds"));
        Assert.Equal(1, await Db.ExpireDueAsync());
        await Db.AssertAccountingAsync(0, 0);
    }

    [Fact]
    public async Task ExpiredActiveHoldIsReleasedWhenCreatingAReplacement()
    {
        await Db.SeedAsync();
        var original = await Db.CreateHoldAsync();
        await MakeExpiredAsync(original.HoldId);
        var replacement = await Db.CreateHoldAsync(key: "replacement");
        Assert.NotEqual(original.HoldId, replacement.HoldId);
        Assert.Equal("Expired", await Db.ScalarAsync<string>("SELECT state FROM capacity_holds WHERE hold_id=@id", new { id = original.HoldId }));
        Assert.Equal(2, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(1, 0);
    }

    [Fact]
    public async Task DifferentKeysCannotCreateTwoActiveHoldsForOneBooking()
    {
        await Db.SeedAsync(10);
        await Db.CreateHoldAsync();
        using var duplicate = await Db.CreateAsync(key: "another-key");
        Assert.Equal(HttpStatusCode.Conflict, duplicate.StatusCode);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(1, 0);
    }

    [Fact]
    public async Task ConfirmedBookingCannotReserveAgainOrReleaseItsConsumedHold()
    {
        await Db.SeedAsync(10);
        var hold = await Db.CreateHoldAsync();
        using var confirmed = await Db.ConfirmAsync(hold.HoldId);
        Assert.Equal(HttpStatusCode.OK, confirmed.StatusCode);
        using var secondConfirm = await Db.ConfirmAsync(hold.HoldId, "different-confirm-key");
        Assert.Equal(HttpStatusCode.OK, secondConfirm.StatusCode);
        Assert.Equal(await confirmed.Content.ReadAsStringAsync(), await secondConfirm.Content.ReadAsStringAsync());
        using var newHold = await Db.CreateAsync(key: "new-create-key");
        Assert.Equal(HttpStatusCode.Conflict, newHold.StatusCode);
        using var cancel = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Delete);
        Assert.Equal(HttpStatusCode.Conflict, cancel.StatusCode);
        Assert.False(await Db.ExpireAsync(hold.HoldId));
        await Db.AssertAccountingAsync(0, 1);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
    }

    [Fact]
    public async Task BookingIdentityCannotMoveCustomerVoyageOrQuantity()
    {
        await Db.SeedAsync(10);
        await Db.SeedAsync(10, voyage: "voyage-2");
        var hold = await Db.CreateHoldAsync();
        using var cancelled = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Delete);
        using var changedVoyage = await Db.CreateAsync(key: "changed-voyage", voyage: "voyage-2");
        using var changedQuantity = await Db.CreateAsync(key: "changed-quantity", quantity: 2);
        using var changedCustomer = await Db.CreateAsync(key: "changed-customer", customer: "customer-2");
        Assert.Equal(HttpStatusCode.Conflict, changedVoyage.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, changedQuantity.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, changedCustomer.StatusCode);
        await Db.AssertAccountingAsync(0, 0);
        await Db.AssertAccountingAsync(0, 0, "voyage-2");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task InvalidQuantitiesAreRejectedWithoutDatabaseEffects(int quantity)
    {
        await Db.SeedAsync();
        using var response = await Db.CreateAsync(quantity: quantity);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(0, 0);
    }

    [Fact]
    public async Task CustomerOwnershipAndRequiredIdentityAreEnforced()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        using var anonymous = await Db.Client.GetAsync($"/api/capacity-holds/{hold.HoldId}");
        Assert.Equal(HttpStatusCode.Unauthorized, anonymous.StatusCode);
        using var get = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Get, "another-customer");
        using var cancel = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Delete, "another-customer");
        using var confirm = await Db.ConfirmAsync(hold.HoldId, customer: "another-customer");
        Assert.Equal(HttpStatusCode.NotFound, get.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, cancel.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, confirm.StatusCode);
        await Db.AssertAccountingAsync(1, 0);
    }

    [Fact]
    public async Task DatabaseConstraintRejectsOversellingEvenOutsideApplicationCode()
    {
        await Db.SeedAsync();
        var error = await Assert.ThrowsAsync<PostgresException>(() =>
            Db.ExecuteAsync("UPDATE voyage_capacity SET reserved=1, confirmed=1"));
        Assert.Equal(PostgresErrorCodes.CheckViolation, error.SqlState);
        await Db.AssertAccountingAsync(0, 0);
    }

    private Task MakeExpiredAsync(Guid id) => Db.ExecuteAsync(
        "UPDATE capacity_holds SET created_at=clock_timestamp()-interval '2 minutes', expires_at=clock_timestamp()-interval '1 second' WHERE hold_id=@id", new { id });
}
