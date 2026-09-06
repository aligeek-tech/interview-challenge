using CapacityBooking.Application;
using Dapper;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CapacityBooking.Infrastructure;

/// <summary>Read-only compatibility and backlog checks with a single bounded connection lease.</summary>
public sealed class DatabaseReadinessProbe(Database database, IOptions<HealthOptions> options)
{
    public async Task<DatabaseHealthSnapshot> CheckAsync(CancellationToken ct = default)
    {
        var timeout = options.Value.DatabaseTimeout;
        if (timeout <= TimeSpan.Zero || timeout > TimeSpan.FromMinutes(1))
            throw new InvalidOperationException("Health database timeout must be positive and at most one minute.");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(timeout);
        var token = deadline.Token;
        try
        {
            await using var connection = await database.OpenAsync(token);
            var applied = (await connection.QueryAsync<AppliedMigration>(new CommandDefinition(
                "SELECT version, sha256 FROM schema_migrations", cancellationToken: token))).ToArray();
            var expected = MigrationRunner.GetMigrations();
            if (applied.Length != expected.Count || expected.Any(m =>
                    !applied.Any(a => a.Version == m.Version && a.Sha256 == m.Sha256)))
                return new(false, "schema_mismatch");

            // Matching history alone cannot detect a missing table/column. Resolve the runtime
            // schema too, without reading business rows or claiming arbitrary drift detection.
            await connection.ExecuteAsync(new CommandDefinition("""
                SELECT voyage_id,total,reserved,confirmed,is_open FROM voyage_capacity LIMIT 0;
                SELECT booking_id,customer_id,voyage_id,quantity,confirmed_hold_id,confirmed_at FROM bookings LIMIT 0;
                SELECT hold_id,booking_id,voyage_id,quantity,state,created_at,expires_at,completed_at FROM capacity_holds LIMIT 0;
                SELECT customer_id,operation,idempotency_key,fingerprint,status_code,response_json FROM idempotency_records LIMIT 0;
                SELECT message_id,event_type,aggregate_id,payload,occurred_at,published_at,attempts,
                    next_attempt_at,lease_token,lease_until,last_error,delivery_state,failure_count,state_version,
                    quarantined_at,quarantine_reason_code FROM outbox LIMIT 0;
                SELECT consumer_name,message_id,processed_at FROM inbox LIMIT 0;
                SELECT message_id,booking_id,voyage_id,hold_id,quantity,confirmed_at FROM booking_confirmations LIMIT 0;
                SELECT booking_id,voyage_id,hold_id,transition,occurred_at,trace_id,details FROM audit_transitions LIMIT 0;
                SELECT action_id,fingerprint,result_json FROM outbox_admin_requests LIMIT 0;
                SELECT message_id,action,attempt,failure_count,state_version,lease_token,action_id,actor,
                    failure_kind,reason_code,reason,occurred_at FROM outbox_delivery_audit LIMIT 0;
                """, cancellationToken: token));
            var backlog = await connection.QuerySingleAsync<Backlog>(new CommandDefinition("""
                SELECT
                    (SELECT count(*) FROM outbox WHERE delivery_state='Pending') AS pending,
                    (SELECT extract(epoch FROM statement_timestamp()-min(occurred_at))::double precision
                        FROM outbox WHERE delivery_state='Pending') AS oldest_outbox_age_seconds,
                    (SELECT count(*) FROM outbox WHERE delivery_state='Quarantined') AS quarantined,
                    (SELECT count(*) FROM capacity_holds WHERE state='Active' AND expires_at<=statement_timestamp()) AS due,
                    (SELECT extract(epoch FROM statement_timestamp()-min(expires_at))::double precision
                        FROM capacity_holds WHERE state='Active' AND expires_at<=statement_timestamp()) AS oldest_due_hold_age_seconds
                """, cancellationToken: token));
            return new(true, "compatible", backlog.Pending, backlog.OldestOutboxAgeSeconds,
                backlog.Quarantined, backlog.Due, backlog.OldestDueHoldAgeSeconds);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return new(false, "database_timeout");
        }
        catch (Exception error) when (error is NpgsqlException or TimeoutException)
        {
            return new(false, error is PostgresException { SqlState: "42P01" or "42703" }
                ? "schema_unavailable" : "database_unavailable");
        }
    }

    private sealed record AppliedMigration(string Version, string Sha256);
    private sealed class Backlog
    {
        public long Pending { get; set; }
        public double? OldestOutboxAgeSeconds { get; set; }
        public long Quarantined { get; set; }
        public long Due { get; set; }
        public double? OldestDueHoldAgeSeconds { get; set; }
    }
}
