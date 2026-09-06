using CapacityBooking.Application;
using CapacityBooking.Infrastructure;
using CapacityBooking.Infrastructure.Reliability;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class OutboxQuarantineTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Theory]
    [InlineData("json", "INVALID_MESSAGE_JSON")]
    [InlineData("event", "UNSUPPORTED_EVENT_TYPE")]
    [InlineData("version", "UNSUPPORTED_SCHEMA_VERSION")]
    [InlineData("identity", "ENVELOPE_IDENTITY_MISMATCH")]
    [InlineData("fields", "INVALID_MESSAGE_FIELDS")]
    public async Task KnownPermanentPoisonQuarantinesWithoutCallingTransport(string defect, string reason)
    {
        var message = await ConfirmOneAsync();
        var payload = defect switch
        {
            "json" => "{malformed",
            "version" => System.Text.Json.JsonSerializer.Serialize(message with { SchemaVersion = 2 }, OperationResult.JsonOptions),
            "identity" => System.Text.Json.JsonSerializer.Serialize(message with { MessageId = Guid.NewGuid() }, OperationResult.JsonOptions),
            "fields" => System.Text.Json.JsonSerializer.Serialize(message with { Quantity = 0 }, OperationResult.JsonOptions),
            _ => System.Text.Json.JsonSerializer.Serialize(message, OperationResult.JsonOptions)
        };
        await Db.ExecuteAsync("UPDATE outbox SET payload=@payload,event_type=@eventType", new
        {
            payload,
            eventType = defect == "event" ? "UnknownEvent" : "BookingConfirmed"
        });
        var calls = 0;
        var publisher = Publisher(new DelegateTransport((_, _) => { calls++; return Task.CompletedTask; }));
        Assert.Equal(0, await publisher.PublishBatchAsync());
        Assert.Equal(0, await publisher.PublishBatchAsync());
        Assert.Equal(0, calls);
        var state = await StateAsync(message.MessageId);
        Assert.Equal("Quarantined", state.DeliveryState);
        Assert.Equal(1, state.Attempts);
        Assert.Equal(1, state.FailureCount);
        Assert.Equal(reason, state.QuarantineReasonCode);
        Assert.Null(state.LeaseToken);
        Assert.Equal(payload, state.Payload);
        Assert.Equal(1, await AuditCountAsync("Quarantined"));
        var listed = Assert.Single(await Administration().ListQuarantinedAsync());
        Assert.Equal(message.MessageId, listed.MessageId);
        Assert.Equal(state.StateVersion, listed.StateVersion);
        Assert.Equal(reason, listed.ReasonCode);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ObservedFailureBudgetPersistsAcrossPublisherInstancesAndStopsFurtherAttempts(bool transient)
    {
        var message = await ConfirmOneAsync();
        var transport = new DelegateTransport((_, _) => Task.FromException(transient
            ? new IOException("Transport unavailable") : new InvalidOperationException("Unclassified defect")));
        Assert.Equal(0, await Publisher(transport, budget: 2).PublishBatchAsync(1));
        var first = await StateAsync(message.MessageId);
        Assert.Equal("Pending", first.DeliveryState);
        Assert.Equal(1, first.FailureCount);
        Assert.True(await Db.ScalarAsync<bool>("SELECT next_attempt_at>clock_timestamp() FROM outbox"));
        await MakeRetryDueAsync();
        Assert.Equal(0, await Publisher(transport, budget: 2).PublishBatchAsync(1));
        Assert.Equal(0, await Publisher(transport, budget: 2).PublishBatchAsync(1));
        var final = await StateAsync(message.MessageId);
        Assert.Equal("Quarantined", final.DeliveryState);
        Assert.Equal(2, final.Attempts);
        Assert.Equal(2, final.FailureCount);
        Assert.Equal("FAILURE_BUDGET_EXHAUSTED", final.QuarantineReasonCode);
        Assert.Equal(1, await AuditCountAsync("RetryScheduled"));
        Assert.Equal(1, await AuditCountAsync("Quarantined"));
        Assert.Equal(transient ? "Transient" : "Unknown", await Db.ScalarAsync<string>(
            "SELECT failure_kind FROM outbox_delivery_audit WHERE action='Quarantined'"));
    }

    [Fact]
    public async Task ShortOutageRecoversWithOriginalIdentityPayloadAndAttemptHistory()
    {
        var message = await ConfirmOneAsync();
        var original = await StateAsync(message.MessageId);
        Assert.Equal(0, await Publisher(new DelegateTransport((_, _) => Task.FromException(new IOException()))).PublishBatchAsync(1));
        await MakeRetryDueAsync();
        Assert.Equal(1, await Publisher().PublishBatchAsync(1));
        var final = await StateAsync(message.MessageId);
        Assert.Equal("Published", final.DeliveryState);
        Assert.Equal(2, final.Attempts);
        Assert.Equal(1, final.FailureCount);
        Assert.Equal(original.Payload, final.Payload);
        Assert.Equal(message.MessageId, await Db.ScalarAsync<Guid>("SELECT message_id FROM booking_confirmations"));
        Assert.Equal(2, await AuditCountAsync("Claimed"));
        Assert.Equal(1, await AuditCountAsync("RetryScheduled"));
        Assert.Equal(1, await AuditCountAsync("Published"));
    }

    [Fact]
    public async Task DeterministicDownstreamIdentityConflictQuarantinesAndRollsBackInbox()
    {
        var message = await ConfirmOneAsync();
        await Db.ExecuteAsync("""
            INSERT INTO booking_confirmations(booking_id,voyage_id,hold_id,quantity,message_id,confirmed_at)
            VALUES (@BookingId,@VoyageId,@otherHold,1,@otherMessage,@ConfirmedAt)
            """, new
        {
            message.BookingId,
            message.VoyageId,
            otherHold = Guid.NewGuid(),
            otherMessage = Guid.NewGuid(),
            message.ConfirmedAt
        });
        Assert.Equal(0, await Publisher(budget: 20).PublishBatchAsync(1));
        var state = await StateAsync(message.MessageId);
        Assert.Equal("Quarantined", state.DeliveryState);
        Assert.Equal("DOWNSTREAM_IDENTITY_CONFLICT", state.QuarantineReasonCode);
        Assert.Equal(1, state.FailureCount);
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
    }

    [Fact]
    public async Task PoisonInOneBatchDoesNotPreventHealthyMessagePublication()
    {
        var poison = await ConfirmOneAsync(capacity: 2);
        var healthy = await Db.CreateHoldAsync(booking: "healthy-booking", key: "healthy-create");
        using var confirmed = await Db.ConfirmAsync(healthy.HoldId, "healthy-confirm");
        Assert.Equal(System.Net.HttpStatusCode.OK, confirmed.StatusCode);
        await Db.ExecuteAsync("UPDATE outbox SET payload='{malformed' WHERE message_id=@MessageId", poison);
        Assert.Equal(1, await Publisher().PublishBatchAsync());
        Assert.Equal("Quarantined", (await StateAsync(poison.MessageId)).DeliveryState);
        Assert.Equal("healthy-booking", await Db.ScalarAsync<string>("SELECT booking_id FROM booking_confirmations"));
        Assert.Equal(1, await AuditCountAsync("Quarantined"));
        Assert.Equal(1, await AuditCountAsync("Published"));
    }

    [Fact]
    public async Task CancellationLeavesLeaseWithoutSpendingTheFailureBudget()
    {
        var message = await ConfirmOneAsync();
        using var cancellation = new CancellationTokenSource();
        var transport = new DelegateTransport((_, ct) =>
        {
            cancellation.Cancel();
            return Task.FromCanceled(ct);
        });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            Publisher(transport, budget: 1).PublishBatchAsync(1, cancellation.Token));
        var interrupted = await StateAsync(message.MessageId);
        Assert.Equal("Pending", interrupted.DeliveryState);
        Assert.Equal(0, interrupted.FailureCount);
        Assert.NotNull(interrupted.LeaseToken);
        await ExpireLeaseAsync();
        Assert.Equal(1, await Publisher(budget: 1).PublishBatchAsync(1));
        Assert.Equal(0, (await StateAsync(message.MessageId)).FailureCount);
    }

    [Fact]
    public async Task ClaimAndItsAuditRollBackTogether()
    {
        var message = await ConfirmOneAsync();
        Db.Observer.FailAt("outbox.before-claim-commit");
        await Assert.ThrowsAsync<InjectedFailureException>(() => Publisher().PublishBatchAsync(1));
        var state = await StateAsync(message.MessageId);
        Assert.Equal(0, state.Attempts);
        Assert.Null(state.LeaseToken);
        Assert.Equal(0, await AuditCountAsync("Claimed"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OutcomeAndAuditFailureLeavesRecoverableClaimWithoutSpendingBudget(bool poison)
    {
        var message = await ConfirmOneAsync();
        if (poison) await Db.ExecuteAsync("UPDATE outbox SET payload='{malformed'");
        Db.Observer.FailAt("outbox.before-outcome-commit");
        await Assert.ThrowsAsync<InjectedFailureException>(() => Publisher(budget: 1).PublishBatchAsync(1));
        var state = await StateAsync(message.MessageId);
        Assert.Equal("Pending", state.DeliveryState);
        Assert.Equal(0, state.FailureCount);
        Assert.Equal(0, state.StateVersion);
        Assert.NotNull(state.LeaseToken);
        Assert.Equal(1, await AuditCountAsync("Claimed"));
        Assert.Equal(0, await AuditCountAsync("Published"));
        Assert.Equal(0, await AuditCountAsync("Quarantined"));
        Db.Observer.Reset();
        await ExpireLeaseAsync();
        Assert.Equal(poison ? 0 : 1, await Publisher(budget: 1).PublishBatchAsync(1));
        Assert.Equal(poison ? "Quarantined" : "Published", (await StateAsync(message.MessageId)).DeliveryState);
        Assert.Equal(poison ? 0 : 1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
    }

    [Fact]
    public async Task ConcurrentIdenticalRedriveReplaysOneActionAndDoesNotRepeatAnAlreadyDeliveredEffect()
    {
        var message = await ConfirmOneAsync();
        Db.Observer.FailAt("outbox.after-publish");
        Assert.Equal(0, await Publisher(budget: 1).PublishBatchAsync(1));
        Db.Observer.Reset();
        var before = await StateAsync(message.MessageId);
        var request = Request(message.MessageId, before.StateVersion);
        using var gate = Db.Observer.Gate("outbox.redrive-before-commit");
        var first = Administration().RedriveAsync(request);
        await gate.WaitForEntryAsync();
        var second = Administration().RedriveAsync(request);
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { gate.Release(); }
        var results = await Task.WhenAll(first, second);
        Assert.All(results, result => Assert.Equal(OutboxRedriveStatus.Redriven, result.Status));
        Assert.Single(results, result => result.Replayed);
        Assert.Equal(results[0].StateVersion, results[1].StateVersion);
        Assert.Equal(1, await AuditCountAsync("Redriven"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox_admin_requests"));
        Db.Observer.Reset();
        var reset = await StateAsync(message.MessageId);
        Assert.Equal("Pending", reset.DeliveryState);
        Assert.Equal(0, reset.FailureCount);
        Assert.Equal(before.Attempts, reset.Attempts);
        Assert.Equal(before.Payload, reset.Payload);
        Assert.Equal(1, await Publisher().PublishBatchAsync(1));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
        var replay = await Administration().RedriveAsync(request);
        Assert.True(replay.Replayed);
        Assert.Equal(results[0].StateVersion, replay.StateVersion);
        var changed = await Administration().RedriveAsync(request with { Reason = "Different operator intent" });
        Assert.Equal(OutboxRedriveStatus.ActionConflict, changed.Status);
    }

    [Fact]
    public async Task ConcurrentDifferentActionsCannotBothRedriveTheSameQuarantineVersion()
    {
        var message = await QuarantineValidMessageAsync();
        var version = (await StateAsync(message.MessageId)).StateVersion;
        using var gate = Db.Observer.Gate("outbox.redrive-before-commit");
        var first = Administration().RedriveAsync(Request(message.MessageId, version));
        await gate.WaitForEntryAsync();
        var second = Administration().RedriveAsync(Request(message.MessageId, version));
        try { await Db.WaitForLockWaitersAsync(1); }
        finally { gate.Release(); }
        var results = await Task.WhenAll(first, second);
        Assert.Single(results, result => result.Status == OutboxRedriveStatus.Redriven);
        Assert.Single(results, result => result.Status == OutboxRedriveStatus.VersionConflict);
        Assert.Equal(1, await AuditCountAsync("Redriven"));
    }

    [Fact]
    public async Task RedriveActionResultAuditAndStateRollBackTogether()
    {
        var message = await QuarantineValidMessageAsync();
        var before = await StateAsync(message.MessageId);
        var request = Request(message.MessageId, before.StateVersion);
        Db.Observer.FailAt("outbox.redrive-before-commit");
        await Assert.ThrowsAsync<InjectedFailureException>(() => Administration().RedriveAsync(request));
        Assert.Equal("Quarantined", (await StateAsync(message.MessageId)).DeliveryState);
        Assert.Equal(before.StateVersion, (await StateAsync(message.MessageId)).StateVersion);
        Assert.Equal(0, await AuditCountAsync("Redriven"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox_admin_requests"));
        Db.Observer.Reset();
        Assert.Equal(OutboxRedriveStatus.Redriven, (await Administration().RedriveAsync(request)).Status);
    }

    [Fact]
    public async Task RejectedAdministrativeResultsReplayAndChangedIntentConflicts()
    {
        var missing = Request(Guid.NewGuid(), 0);
        Assert.Equal(OutboxRedriveStatus.NotFound, (await Administration().RedriveAsync(missing)).Status);
        var replay = await Administration().RedriveAsync(missing);
        Assert.Equal(OutboxRedriveStatus.NotFound, replay.Status);
        Assert.True(replay.Replayed);
        Assert.Equal(OutboxRedriveStatus.ActionConflict,
            (await Administration().RedriveAsync(missing with { ExpectedVersion = 1 })).Status);
        var pending = await ConfirmOneAsync();
        var request = Request(pending.MessageId, 0);
        Assert.Equal(OutboxRedriveStatus.NotQuarantined, (await Administration().RedriveAsync(request)).Status);
        Assert.True((await Administration().RedriveAsync(request)).Replayed);
        Assert.Equal(0, await AuditCountAsync("Redriven"));
    }

    [Fact]
    public async Task DatabaseCannotCommitAnIncompleteAdministrativeClaim()
    {
        var error = await Assert.ThrowsAsync<PostgresException>(() => Db.ExecuteAsync("""
            INSERT INTO outbox_admin_requests(action_id,fingerprint) VALUES(@action,@fingerprint)
            """, new { action = Guid.NewGuid(), fingerprint = new string('A', 64) }));
        Assert.Equal(PostgresErrorCodes.CheckViolation, error.SqlState);
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox_admin_requests"));
    }

    [Fact]
    public async Task StaleOperatorVersionCannotRedriveANewerQuarantineCycle()
    {
        var message = await QuarantineValidMessageAsync();
        var oldVersion = (await StateAsync(message.MessageId)).StateVersion;
        Assert.Equal(OutboxRedriveStatus.Redriven, (await Administration().RedriveAsync(Request(message.MessageId, oldVersion))).Status);
        Assert.Equal(0, await Publisher(PermanentTransport()).PublishBatchAsync(1));
        var newVersion = (await StateAsync(message.MessageId)).StateVersion;
        Assert.True(newVersion > oldVersion);
        var stale = Request(message.MessageId, oldVersion);
        Assert.Equal(OutboxRedriveStatus.VersionConflict, (await Administration().RedriveAsync(stale)).Status);
        Assert.True((await Administration().RedriveAsync(stale)).Replayed);
        Assert.Equal("Quarantined", (await StateAsync(message.MessageId)).DeliveryState);
    }

    [Fact]
    public async Task OldLeaseCannotMarkOrQuarantineAfterANewOwnerQuarantinesAndAnOperatorRedrives()
    {
        var message = await ConfirmOneAsync();
        using var gate = new ObserverGate();
        var deliveries = 0;
        Db.Observer.Configure(async (point, _, ct) =>
        {
            if (point == "outbox.after-publish" && Interlocked.Increment(ref deliveries) == 1)
            {
                await gate.EnterAsync(ct);
                throw new DeliveryFailureException(DeliveryFailureKind.Permanent, "OLD_OWNER_FAILURE");
            }
        });
        var old = Publisher().PublishBatchAsync(1);
        await gate.WaitForEntryAsync();
        await ExpireLeaseAsync();
        Assert.Equal(0, await Publisher(PermanentTransport()).PublishBatchAsync(1));
        var quarantined = await StateAsync(message.MessageId);
        Assert.Equal(OutboxRedriveStatus.Redriven,
            (await Administration().RedriveAsync(Request(message.MessageId, quarantined.StateVersion))).Status);
        gate.Release();
        Assert.Equal(0, await old);
        var afterStale = await StateAsync(message.MessageId);
        Assert.Equal("Pending", afterStale.DeliveryState);
        Assert.Equal(0, afterStale.FailureCount);
        Assert.Equal(1, await AuditCountAsync("StaleOutcomeIgnored"));
        Db.Observer.Reset();
        Assert.Equal(1, await Publisher().PublishBatchAsync(1));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
    }

    [Fact]
    public async Task FreshProcessSkipsQuarantineWhilePublishingHealthyWork()
    {
        var poison = await QuarantineValidMessageAsync(capacity: 2);
        var hold = await Db.CreateHoldAsync(booking: "healthy-booking", key: "healthy-create");
        using var confirmed = await Db.ConfirmAsync(hold.HoldId, "healthy-confirm");
        Assert.Equal(System.Net.HttpStatusCode.OK, confirmed.StatusCode);
        await using var process = await ApiProcess.StartAsync(Db.ConnectionString, workers: true);
        await DatabaseFixture.WaitUntilAsync(() => Db.ScalarAsync<bool>(
            "SELECT delivery_state='Published' FROM outbox WHERE aggregate_id='healthy-booking'"),
            "Fresh process did not publish the healthy message alongside quarantine.");
        var retained = await StateAsync(poison.MessageId);
        Assert.Equal("Quarantined", retained.DeliveryState);
        Assert.Equal(1, retained.Attempts);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
    }

    [Theory]
    [InlineData("UPDATE outbox SET delivery_state='Published'")]
    [InlineData("UPDATE outbox SET delivery_state='Quarantined'")]
    [InlineData("UPDATE outbox SET failure_count=-1")]
    [InlineData("UPDATE outbox SET state_version=-1")]
    public async Task DatabaseRejectsInconsistentDeliveryState(string sql)
    {
        await ConfirmOneAsync();
        var error = await Assert.ThrowsAsync<PostgresException>(() => Db.ExecuteAsync(sql));
        Assert.Equal(PostgresErrorCodes.CheckViolation, error.SqlState);
    }

    private OutboxPublisher Publisher(IMessageTransport? transport = null, int budget = 3) => new(
        new Database(Db.ConnectionString), transport ?? new LocalMessageTransport(new BookingConfirmedConsumer(
            new Database(Db.ConnectionString), Db.Observer, NullLogger<BookingConfirmedConsumer>.Instance)),
        Options.Create(new DeliveryOptions
        {
            LeaseDuration = TimeSpan.FromMinutes(1),
            RetryBaseDelay = TimeSpan.FromHours(1),
            MaxRetryDelay = TimeSpan.FromHours(1),
            MaxFailuresPerCycle = budget
        }), Db.Observer, NullLogger<OutboxPublisher>.Instance);

    private OutboxAdministration Administration() => new(new Database(Db.ConnectionString), Db.Observer);
    private static OutboxRedriveRequest Request(Guid id, long version) =>
        new(id, version, Guid.NewGuid(), "operator-test", "Verified the delivery configuration and restored the dependency.");
    private static IMessageTransport PermanentTransport() => new DelegateTransport((_, _) => Task.FromException(
        new DeliveryFailureException(DeliveryFailureKind.Permanent, "DELIVERY_CONFIGURATION_REJECTED")));
    private Task MakeRetryDueAsync() => Db.ExecuteAsync("UPDATE outbox SET next_attempt_at=clock_timestamp() WHERE delivery_state='Pending'");
    private Task ExpireLeaseAsync() => Db.ExecuteAsync("UPDATE outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE lease_token IS NOT NULL");
    private Task<int> AuditCountAsync(string action) => Db.ScalarAsync<int>(
        "SELECT count(*)::int FROM outbox_delivery_audit WHERE action=@action", new { action });
    private Task<OutboxState> StateAsync(Guid id) => Db.QuerySingleAsync<OutboxState>(
        "SELECT delivery_state,attempts,failure_count,state_version,quarantine_reason_code,lease_token,payload FROM outbox WHERE message_id=@id", new { id });

    private async Task<BookingConfirmedMessage> ConfirmOneAsync(int capacity = 1)
    {
        await Db.SeedAsync(capacity);
        var hold = await Db.CreateHoldAsync();
        using var confirmed = await Db.ConfirmAsync(hold.HoldId);
        Assert.Equal(System.Net.HttpStatusCode.OK, confirmed.StatusCode);
        return await Db.OutboxMessageAsync();
    }

    private async Task<BookingConfirmedMessage> QuarantineValidMessageAsync(int capacity = 1)
    {
        var message = await ConfirmOneAsync(capacity);
        Assert.Equal(0, await Publisher(PermanentTransport()).PublishBatchAsync(1));
        return message;
    }

    private sealed class DelegateTransport(Func<BookingConfirmedMessage, CancellationToken, Task> publish) : IMessageTransport
    {
        public Task PublishAsync(BookingConfirmedMessage message, CancellationToken ct = default) => publish(message, ct);
    }

    public sealed class OutboxState
    {
        public string DeliveryState { get; set; } = "";
        public int Attempts { get; set; }
        public int FailureCount { get; set; }
        public long StateVersion { get; set; }
        public string? QuarantineReasonCode { get; set; }
        public Guid? LeaseToken { get; set; }
        public string Payload { get; set; } = "";
    }
}
