using System.Data;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CapacityBooking.Application;
using Dapper;

namespace CapacityBooking.Infrastructure.Reliability;

/// <summary>Operator re-drive changes scheduling only; message identity and payload are immutable here.</summary>
public sealed class OutboxAdministration(Database database, IExecutionObserver? observer = null) : IOutboxAdministration
{
    public async Task<IReadOnlyList<OutboxQuarantineItem>> ListQuarantinedAsync(int limit = 100, CancellationToken ct = default)
    {
        if (limit is < 1 or > 1000)
            throw new ArgumentOutOfRangeException(nameof(limit), "List limit must be between 1 and 1000.");
        await using var connection = await database.OpenAsync(ct);
        var rows = await connection.QueryAsync<QuarantineRow>(new CommandDefinition("""
            SELECT message_id, event_type, aggregate_id, attempts, failure_count, state_version,
                   quarantined_at, quarantine_reason_code
            FROM outbox WHERE delivery_state='Quarantined'
            ORDER BY quarantined_at, message_id LIMIT @Limit
            """, new { Limit = limit }, cancellationToken: ct));
        return rows.Select(row => new OutboxQuarantineItem(row.MessageId, row.EventType, row.AggregateId,
            row.Attempts, row.FailureCount, row.StateVersion,
            new DateTimeOffset(row.QuarantinedAt, TimeSpan.Zero), row.QuarantineReasonCode)).ToArray();
    }

    public async Task<OutboxRedriveResult> RedriveAsync(OutboxRedriveRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.MessageId == Guid.Empty || request.ActionId == Guid.Empty || request.ExpectedVersion < 0 ||
            !IsBoundedText(request.Actor, 128) || !IsBoundedText(request.Reason, 512))
            return new(OutboxRedriveStatus.InvalidRequest, request.MessageId, null);

        var fingerprint = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
            JsonSerializer.Serialize(request, OperationResult.JsonOptions))));
        await using var connection = await database.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var claimed = await connection.ExecuteAsync(new CommandDefinition("""
            INSERT INTO outbox_admin_requests(action_id,fingerprint)
            VALUES (@ActionId,@Fingerprint) ON CONFLICT(action_id) DO NOTHING
            """, new { request.ActionId, Fingerprint = fingerprint }, transaction, cancellationToken: ct));
        if (claimed == 0)
        {
            var existing = await connection.QuerySingleAsync<AdminRequestRow>(new CommandDefinition("""
                SELECT fingerprint,result_json FROM outbox_admin_requests WHERE action_id=@ActionId
                """, new { request.ActionId }, transaction, cancellationToken: ct));
            await transaction.CommitAsync(ct);
            if (existing.Fingerprint != fingerprint)
                return new(OutboxRedriveStatus.ActionConflict, request.MessageId, null);
            return (JsonSerializer.Deserialize<OutboxRedriveResult>(existing.ResultJson, OperationResult.JsonOptions)
                ?? throw new InvalidOperationException("An administrative result was not completed.")) with
            { Replayed = true };
        }

        var row = await connection.QuerySingleOrDefaultAsync<StateRow>(new CommandDefinition("""
            SELECT delivery_state,state_version,attempts,failure_count
            FROM outbox WHERE message_id=@MessageId FOR UPDATE
            """, new { request.MessageId }, transaction, cancellationToken: ct));
        OutboxRedriveResult result;
        if (row is null)
            result = new(OutboxRedriveStatus.NotFound, request.MessageId, null);
        else if (row.StateVersion != request.ExpectedVersion)
            result = new(OutboxRedriveStatus.VersionConflict, request.MessageId, row.StateVersion);
        else if (row.DeliveryState != "Quarantined")
            result = new(OutboxRedriveStatus.NotQuarantined, request.MessageId, row.StateVersion);
        else
        {
            var nextVersion = checked(row.StateVersion + 1);
            var updated = await connection.ExecuteAsync(new CommandDefinition("""
                UPDATE outbox SET delivery_state='Pending',failure_count=0,state_version=@NextVersion,
                    quarantined_at=NULL,quarantine_reason_code=NULL,last_error=NULL,
                    lease_token=NULL,lease_until=NULL,next_attempt_at=clock_timestamp()
                WHERE message_id=@MessageId AND delivery_state='Quarantined' AND state_version=@ExpectedVersion
                """, new { request.MessageId, request.ExpectedVersion, NextVersion = nextVersion }, transaction, cancellationToken: ct));
            if (updated != 1)
                throw new InvalidOperationException("The locked quarantine version changed unexpectedly.");
            await connection.ExecuteAsync(new CommandDefinition("""
                INSERT INTO outbox_delivery_audit
                    (message_id,action,attempt,failure_count,state_version,action_id,actor,reason_code,reason)
                VALUES (@MessageId,'Redriven',@Attempts,0,@NextVersion,@ActionId,@Actor,'OPERATOR_REDRIVE',@Reason)
                """, new
            {
                request.MessageId,
                row.Attempts,
                NextVersion = nextVersion,
                request.ActionId,
                request.Actor,
                request.Reason
            }, transaction, cancellationToken: ct));
            result = new(OutboxRedriveStatus.Redriven, request.MessageId, nextVersion);
        }

        await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE outbox_admin_requests SET result_json=@ResultJson WHERE action_id=@ActionId
            """, new { request.ActionId, ResultJson = JsonSerializer.Serialize(result, OperationResult.JsonOptions) },
            transaction, cancellationToken: ct));
        if (observer is not null)
            await observer.ReachedAsync("outbox.redrive-before-commit", request.MessageId.ToString("D"), ct);
        await transaction.CommitAsync(ct);
        return result;
    }

    private static bool IsBoundedText(string? value, int maximum) => !string.IsNullOrWhiteSpace(value) &&
        value.Length <= maximum && !value.Any(char.IsControl);

    private sealed class QuarantineRow
    {
        public Guid MessageId { get; set; }
        public string EventType { get; set; } = "";
        public string AggregateId { get; set; } = "";
        public int Attempts { get; set; }
        public int FailureCount { get; set; }
        public long StateVersion { get; set; }
        public DateTime QuarantinedAt { get; set; }
        public string QuarantineReasonCode { get; set; } = "";
    }

    private sealed class StateRow
    {
        public string DeliveryState { get; set; } = "";
        public long StateVersion { get; set; }
        public int Attempts { get; set; }
        public int FailureCount { get; set; }
    }

    private sealed class AdminRequestRow
    {
        public string Fingerprint { get; set; } = "";
        public string ResultJson { get; set; } = "";
    }
}
