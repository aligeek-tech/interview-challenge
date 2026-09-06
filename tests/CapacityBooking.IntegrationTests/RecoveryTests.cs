using System.Net;
using CapacityBooking.Application;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class RecoveryTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Theory]
    [InlineData("confirm.after-decision")]
    [InlineData("confirm.before-commit")]
    public async Task ConfirmationFailureRollsBackBookingCountersAuditOutboxAndIdempotency(string point)
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        var previousAuditCount = await Db.ScalarAsync<int>("SELECT count(*)::int FROM audit_transitions");
        Db.Observer.FailAt(point);
        await Assert.ThrowsAsync<InjectedFailureException>(() => Db.WithServiceAsync<IBookingService, OperationResult>(
            service => service.ConfirmAsync(hold.HoldId, "failing-confirm", new RequestContext("customer-1", "rollback-proof"))));
        Db.Observer.Reset();
        await Db.AssertAccountingAsync(1, 0);
        Assert.Equal("Active", await Db.ScalarAsync<string>("SELECT state FROM capacity_holds"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM bookings WHERE confirmed_hold_id IS NOT NULL"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
        Assert.Equal(previousAuditCount, await Db.ScalarAsync<int>("SELECT count(*)::int FROM audit_transitions"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM idempotency_records WHERE idempotency_key='failing-confirm'"));
        using var retry = await Db.ConfirmAsync(hold.HoldId, "failing-confirm");
        Assert.Equal(HttpStatusCode.OK, retry.StatusCode);
        await Db.AssertAccountingAsync(0, 1);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
    }

    [Fact]
    public async Task FailedPublicationLeavesConfirmationCommittedAndEventAvailableForRetry()
    {
        await ConfirmOneAsync();
        var message = await Db.OutboxMessageAsync();
        Db.Observer.FailAt("consumer.before-commit");
        Assert.Equal(0, await Db.PublishAsync());
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE published_at IS NULL AND attempts>=1 AND next_attempt_at>occurred_at"));
        await Db.AssertAccountingAsync(0, 1);
        Db.Observer.Reset();
        await MakeRetryDueAsync();
        Assert.Equal(1, await Db.PublishAsync());
        Assert.Equal(message.MessageId, await Db.ScalarAsync<Guid>("SELECT message_id FROM outbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE published_at IS NOT NULL"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
    }

    [Fact]
    public async Task FailureAfterPublishBeforeMarkRedeliversSameMessageWithOneDownstreamEffect()
    {
        await ConfirmOneAsync();
        var message = await Db.OutboxMessageAsync();
        Db.Observer.FailAt("outbox.after-publish");
        Assert.Equal(0, await Db.PublishAsync());
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE published_at IS NULL"));
        Db.Observer.Reset();
        await MakeRetryDueAsync();
        Assert.Equal(1, await Db.PublishAsync());
        Assert.Equal(message.MessageId, await Db.ScalarAsync<Guid>("SELECT message_id FROM booking_confirmations"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE published_at IS NOT NULL"));
    }

    [Fact]
    public async Task DuplicateEventConsumptionHasOneEffectiveResult()
    {
        await ConfirmOneAsync();
        var message = await Db.OutboxMessageAsync();
        Assert.True(await Db.ConsumeAsync(message));
        Assert.False(await Db.ConsumeAsync(message));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
        Assert.Equal(message.HoldId, await Db.ScalarAsync<Guid>("SELECT hold_id FROM booking_confirmations"));
    }

    [Fact]
    public async Task InboxAndProjectionRollBackTogetherAndRetryCanApplyTheMessage()
    {
        await ConfirmOneAsync();
        var message = await Db.OutboxMessageAsync();
        Db.Observer.FailAt("consumer.before-commit");
        await Assert.ThrowsAsync<InjectedFailureException>(() => Db.ConsumeAsync(message));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
        Db.Observer.Reset();
        Assert.True(await Db.ConsumeAsync(message));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
    }

    [Fact]
    public async Task ConcurrentDuplicateDeliveriesWaitOnInboxAndProduceOneEffect()
    {
        await ConfirmOneAsync();
        var message = await Db.OutboxMessageAsync();
        using var gate = Db.Observer.Gate("consumer.before-commit", message.MessageId.ToString());
        var first = Db.ConsumeAsync(message);
        await gate.WaitForEntryAsync();
        var second = Db.ConsumeAsync(message);
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { gate.Release(); }
        Assert.True(await first);
        Assert.False(await second);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
    }

    [Fact]
    public async Task AbandonedOutboxLeaseCanBeReclaimedAfterItsDeadline()
    {
        await ConfirmOneAsync();
        await Db.ExecuteAsync("UPDATE outbox SET lease_token=@token, lease_until=clock_timestamp()+interval '1 hour'", new { token = Guid.NewGuid() });
        Assert.Equal(0, await Db.PublishAsync());
        await Db.ExecuteAsync("UPDATE outbox SET lease_until=clock_timestamp()-interval '1 second'");
        Assert.Equal(1, await Db.PublishAsync());
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE published_at IS NOT NULL AND lease_token IS NULL"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StalePublisherCannotOverwriteAReclaimedLeaseOrRescheduleAnAlreadyPublishedMessage(bool oldAttemptFails)
    {
        await ConfirmOneAsync();
        using var firstDelivery = new ObserverGate();
        var deliveries = 0;
        Db.Observer.Configure(async (point, _, ct) =>
        {
            if (point == "outbox.after-publish" && Interlocked.Increment(ref deliveries) == 1)
            {
                await firstDelivery.EnterAsync(ct);
                if (oldAttemptFails) throw new InjectedFailureException(point);
            }
        });
        var stalePublisher = Db.WithServiceAsync<IOutboxPublisher, int>(service => service.PublishBatchAsync(1));
        await firstDelivery.WaitForEntryAsync();
        await Db.ExecuteAsync("UPDATE outbox SET lease_until=clock_timestamp()-interval '1 second'");
        try
        {
            Assert.Equal(1, await Db.WithServiceAsync<IOutboxPublisher, int>(service => service.PublishBatchAsync(1)));
        }
        finally { firstDelivery.Release(); }
        Assert.Equal(0, await stalePublisher);
        Assert.Equal(2, deliveries);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE published_at IS NOT NULL AND lease_token IS NULL AND last_error IS NULL"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
    }

    private Task MakeRetryDueAsync() => Db.ExecuteAsync("UPDATE outbox SET next_attempt_at=clock_timestamp(), lease_until=NULL, lease_token=NULL WHERE published_at IS NULL");

    private async Task ConfirmOneAsync()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        using var response = await Db.ConfirmAsync(hold.HoldId);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
