using System.Data;
using System.Diagnostics;
using CapacityBooking.Application;
using CapacityBooking.Domain;
using Dapper;
using Microsoft.Extensions.Logging;

namespace CapacityBooking.Infrastructure;

/// <summary>Reconciles persisted deadlines; there is no process-local expiry queue.</summary>
public sealed class ExpiryService(
    Database database,
    IExecutionObserver observer,
    ILogger<ExpiryService> logger) : IExpiryService
{
    public async Task<int> ExpireDueAsync(int batchSize = 100, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        Guid[] candidates;
        await using (var connection = await database.OpenAsync(ct))
        {
            // Do not claim/lock a hold here: every mutator locks voyage, booking, then hold.
            candidates = (await connection.QueryAsync<Guid>(new CommandDefinition("""
                SELECT hold_id
                FROM capacity_holds
                WHERE state = 'Active' AND expires_at <= statement_timestamp()
                ORDER BY expires_at, hold_id
                LIMIT @BatchSize
                """, new { BatchSize = batchSize }, cancellationToken: ct))).ToArray();
        }

        var expired = 0;
        foreach (var holdId in candidates)
        {
            if (await ExpireAsync(holdId, ct))
                expired++;
        }

        return expired;
    }

    public async Task<bool> ExpireAsync(Guid holdId, CancellationToken ct = default)
    {
        await using var connection = await database.OpenAsync(ct);
        // Identity fields are immutable. This read locates the first lock in the global order.
        var identity = await connection.QuerySingleOrDefaultAsync<HoldIdentity>(new CommandDefinition("""
            SELECT booking_id, voyage_id
            FROM capacity_holds WHERE hold_id = @HoldId
            """, new { HoldId = holdId }, cancellationToken: ct));
        if (identity is null)
            return false;

        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        await connection.QuerySingleAsync<string>(new CommandDefinition("""
            SELECT voyage_id FROM voyage_capacity WHERE voyage_id = @VoyageId FOR UPDATE
            """, new { identity.VoyageId }, transaction, cancellationToken: ct));
        await connection.QuerySingleAsync<string>(new CommandDefinition("""
            SELECT booking_id FROM bookings WHERE booking_id = @BookingId FOR UPDATE
            """, new { identity.BookingId }, transaction, cancellationToken: ct));
        var hold = await connection.QuerySingleAsync<HoldRow>(new CommandDefinition("""
            SELECT hold_id, booking_id, voyage_id, quantity, state, expires_at
            FROM capacity_holds WHERE hold_id = @HoldId FOR UPDATE
            """, new { HoldId = holdId }, transaction, cancellationToken: ct));

        await observer.ReachedAsync("business.after-locks", holdId.ToString("D"), ct);
        // CURRENT_TIMESTAMP is frozen at transaction start, possibly before a long lock wait.
        var decisionTime = await connection.QuerySingleAsync<DateTime>(new CommandDefinition(
            "SELECT clock_timestamp()", transaction: transaction, cancellationToken: ct));

        var deadline = new HoldDeadline(new DateTimeOffset(hold.ExpiresAt, TimeSpan.Zero));
        if (hold.State != "Active" || !deadline.IsExpiredAt(new DateTimeOffset(decisionTime, TimeSpan.Zero)))
        {
            await transaction.CommitAsync(ct);
            logger.LogInformation(
                "Expiry made no transition for HoldId {HoldId}, BookingId {BookingId}, VoyageId {VoyageId}: State {State}, DecisionTime {DecisionTime}, ExpiresAt {ExpiresAt}",
                holdId, hold.BookingId, hold.VoyageId, hold.State, decisionTime, hold.ExpiresAt);
            return false;
        }

        var capacityUpdated = await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE voyage_capacity SET reserved = reserved - @Quantity
            WHERE voyage_id = @VoyageId AND reserved >= @Quantity
            """, new { hold.Quantity, hold.VoyageId }, transaction, cancellationToken: ct));
        if (capacityUpdated != 1)
            throw new InvalidOperationException("Persisted reserved capacity cannot cover an active hold.");

        var holdUpdated = await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE capacity_holds
            SET state = 'Expired', completed_at = @DecisionTime
            WHERE hold_id = @HoldId AND state = 'Active'
            """, new { HoldId = holdId, DecisionTime = decisionTime }, transaction, cancellationToken: ct));
        if (holdUpdated != 1)
            throw new InvalidOperationException("The locked active hold could not transition to Expired.");

        var traceId = Activity.Current?.TraceId.ToString() ?? "expiry-worker";
        await connection.ExecuteAsync(new CommandDefinition("""
            INSERT INTO audit_transitions
                (booking_id, voyage_id, hold_id, transition, occurred_at, trace_id, details)
            VALUES
                (@BookingId, @VoyageId, @HoldId, 'CapacityHoldExpired', @DecisionTime, @TraceId,
                 'Capacity returned after the hold deadline.')
            """, new
        {
            hold.BookingId,
            hold.VoyageId,
            HoldId = holdId,
            DecisionTime = decisionTime,
            TraceId = traceId
        }, transaction, cancellationToken: ct));

        await transaction.CommitAsync(ct);
        logger.LogInformation(
            "CapacityHoldExpired: HoldId {HoldId}, BookingId {BookingId}, VoyageId {VoyageId}, Quantity {Quantity}, DecisionTime {DecisionTime}, TraceId {TraceId}",
            holdId, hold.BookingId, hold.VoyageId, hold.Quantity, decisionTime, traceId);
        return true;
    }

    private sealed class HoldIdentity
    {
        public string BookingId { get; set; } = "";
        public string VoyageId { get; set; } = "";
    }

    private sealed class HoldRow
    {
        public Guid HoldId { get; set; }
        public string BookingId { get; set; } = "";
        public string VoyageId { get; set; } = "";
        public int Quantity { get; set; }
        public string State { get; set; } = "";
        public DateTime ExpiresAt { get; set; }
    }
}
