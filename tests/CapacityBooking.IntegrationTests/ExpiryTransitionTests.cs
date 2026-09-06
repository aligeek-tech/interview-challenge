using System.Net;
using CapacityBooking.Application;
using CapacityBooking.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class ExpiryTransitionTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Theory]
    [InlineData("scheduled")]
    [InlineData("confirm")]
    [InlineData("replacement")]
    [InlineData("cancel")]
    public async Task EveryExpiryTriggerHasTheSameTerminalStateAccountingAndSingleAudit(string trigger)
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        await MakeDueAsync(hold.HoldId);
        switch (trigger)
        {
            case "scheduled":
                Assert.Equal(1, await Db.ExpireDueAsync());
                break;
            case "confirm":
                using (var response = await Db.ConfirmAsync(hold.HoldId))
                    Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
                break;
            case "replacement":
                using (var response = await Db.CreateAsync(key: "replacement"))
                    Assert.Equal(HttpStatusCode.Created, response.StatusCode);
                break;
            case "cancel":
                using (var response = await Db.HoldRequestAsync(hold.HoldId, HttpMethod.Delete))
                    Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                break;
        }
        Assert.Equal("Expired", await Db.ScalarAsync<string>("SELECT state FROM capacity_holds WHERE hold_id=@id", new { id = hold.HoldId }));
        Assert.True(await Db.ScalarAsync<bool>("SELECT completed_at>=expires_at FROM capacity_holds WHERE hold_id=@id", new { id = hold.HoldId }));
        Assert.Equal(1, await AuditCountAsync(hold.HoldId));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::integer FROM outbox"));
        await Db.AssertAccountingAsync(trigger == "replacement" ? 1 : 0, 0);
        Assert.False(await Db.ExpireAsync(hold.HoldId));
        Assert.Equal(1, await AuditCountAsync(hold.HoldId));
    }

    [Fact]
    public async Task FailureAfterSharedExpiryWritesRollsBackCapacityHoldAndAuditTogether()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        await MakeDueAsync(hold.HoldId);
        var observer = new TestObserver();
        observer.FailAt("expiry.before-commit");
        var service = new ExpiryService(new Database(Db.ConnectionString), observer, NullLogger<ExpiryService>.Instance);
        await Assert.ThrowsAsync<InjectedFailureException>(() => service.ExpireAsync(hold.HoldId));
        await Db.AssertAccountingAsync(1, 0);
        Assert.Equal("Active", await Db.ScalarAsync<string>("SELECT state FROM capacity_holds WHERE hold_id=@id", new { id = hold.HoldId }));
        Assert.Equal(0, await AuditCountAsync(hold.HoldId));
        observer.Reset();
        Assert.True(await service.ExpireAsync(hold.HoldId));
        Assert.False(await service.ExpireAsync(hold.HoldId));
        await Db.AssertAccountingAsync(0, 0);
        Assert.Equal(1, await AuditCountAsync(hold.HoldId));
    }

    private Task MakeDueAsync(Guid id) => Db.ExecuteAsync("""
        UPDATE capacity_holds
        SET created_at=clock_timestamp()-interval '2 minutes', expires_at=clock_timestamp()-interval '1 second'
        WHERE hold_id=@id
        """, new { id });
    private Task<int> AuditCountAsync(Guid id) => Db.ScalarAsync<int>(
        "SELECT count(*)::integer FROM audit_transitions WHERE hold_id=@id AND transition='CapacityHoldExpired'", new { id });
}
