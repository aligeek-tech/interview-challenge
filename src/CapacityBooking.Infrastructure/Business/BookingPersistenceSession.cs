using System.Globalization;
using System.Text.Json;
using CapacityBooking.Application;
using CapacityBooking.Domain;
using Dapper;
using Npgsql;

namespace CapacityBooking.Infrastructure.Business;

/// <summary>
/// PostgreSQL repository operations on the caller's connection/transaction. This
/// object never opens a connection or commits, so composing operations is atomic.
/// </summary>
internal sealed class BookingPersistenceSession(NpgsqlConnection connection, NpgsqlTransaction? transaction = null)
{
    public async Task<VoyageCapacity?> LockCapacityAsync(string voyageId, CancellationToken ct, bool skipLocked = false)
    {
        var row = await connection.QuerySingleOrDefaultAsync<CapacityRow>(Command("""
            SELECT voyage_id, total, reserved, confirmed, is_open
            FROM voyage_capacity WHERE voyage_id = @VoyageId FOR UPDATE
            """ + (skipLocked ? " SKIP LOCKED" : ""), new { VoyageId = voyageId }, ct));
        return row is null ? null : new VoyageCapacity(row.VoyageId, row.Total, row.Reserved, row.Confirmed, row.IsOpen);
    }

    public async Task<Booking?> LockBookingAsync(string bookingId, CancellationToken ct)
    {
        var row = await connection.QuerySingleOrDefaultAsync<BookingRow>(Command("""
            SELECT booking_id, voyage_id, customer_id, quantity, confirmed_hold_id, confirmed_at
            FROM bookings WHERE booking_id = @BookingId FOR UPDATE
            """, new { BookingId = bookingId }, ct));
        return row is null ? null : new Booking(row.BookingId, row.VoyageId, row.CustomerId,
            new CapacityUnits(row.Quantity), row.ConfirmedHoldId, row.ConfirmedAt is { } at ? Utc(at) : null);
    }

    public async Task<CapacityHold?> LockHoldAsync(Guid holdId, CancellationToken ct)
    {
        var row = await connection.QuerySingleOrDefaultAsync<HoldRow>(Command("""
            SELECT hold_id, booking_id, voyage_id, quantity, state, created_at, expires_at, completed_at
            FROM capacity_holds WHERE hold_id = @HoldId FOR UPDATE
            """, new { HoldId = holdId }, ct));
        return row?.ToDomain();
    }

    public async Task<CapacityHold?> LockActiveHoldAsync(string bookingId, string voyageId, CancellationToken ct)
    {
        var row = await connection.QuerySingleOrDefaultAsync<HoldRow>(Command("""
            SELECT hold_id, booking_id, voyage_id, quantity, state, created_at, expires_at, completed_at
            FROM capacity_holds
            WHERE booking_id = @BookingId AND voyage_id = @VoyageId AND state = 'Active'
            FOR UPDATE
            """, new { BookingId = bookingId, VoyageId = voyageId }, ct));
        return row?.ToDomain();
    }

    public Task<HoldIdentity?> FindOwnedHoldIdentityAsync(Guid holdId, string customerId, CancellationToken ct) =>
        connection.QuerySingleOrDefaultAsync<HoldIdentity>(Command("""
            SELECT h.booking_id, h.voyage_id
            FROM capacity_holds h JOIN bookings b ON b.booking_id = h.booking_id
            WHERE h.hold_id = @HoldId AND b.customer_id = @CustomerId
            """, new { HoldId = holdId, CustomerId = customerId }, ct));

    public Task<HoldIdentity?> FindHoldIdentityAsync(Guid holdId, CancellationToken ct) =>
        connection.QuerySingleOrDefaultAsync<HoldIdentity>(Command("""
            SELECT booking_id, voyage_id FROM capacity_holds WHERE hold_id = @HoldId
            """, new { HoldId = holdId }, ct));

    public Task<int> InsertBookingIfAbsentAsync(string bookingId, string voyageId, string customerId, int quantity, CancellationToken ct) =>
        connection.ExecuteAsync(Command("""
            INSERT INTO bookings (booking_id, voyage_id, customer_id, quantity)
            VALUES (@BookingId, @VoyageId, @CustomerId, @Quantity)
            ON CONFLICT (booking_id) DO NOTHING
            """, new { BookingId = bookingId, VoyageId = voyageId, CustomerId = customerId, Quantity = quantity }, ct));

    public Task<int> InsertHoldAsync(CapacityHold hold, CancellationToken ct) =>
        connection.ExecuteAsync(Command("""
            INSERT INTO capacity_holds
                (hold_id, booking_id, voyage_id, quantity, state, created_at, expires_at)
            VALUES (@HoldId, @BookingId, @VoyageId, @Quantity, 'Active', @CreatedAt, @ExpiresAt)
            """, new
        {
            hold.HoldId,
            hold.BookingId,
            hold.VoyageId,
            Quantity = hold.Quantity.Value,
            hold.CreatedAt,
            ExpiresAt = hold.Deadline.ExpiresAt
        }, ct));

    public async Task SaveCapacityAsync(VoyageCapacity capacity, CancellationToken ct)
    {
        var count = await connection.ExecuteAsync(Command("""
            UPDATE voyage_capacity SET reserved = @Reserved, confirmed = @Confirmed
            WHERE voyage_id = @VoyageId
            """, new { capacity.VoyageId, capacity.Reserved, capacity.Confirmed }, ct));
        RequireSingleWrite(count, "Capacity mutation");
    }

    public async Task SaveTransitionAsync(CapacityHold hold, CancellationToken ct)
    {
        var count = await connection.ExecuteAsync(Command("""
            UPDATE capacity_holds SET state = @State, completed_at = @CompletedAt
            WHERE hold_id = @HoldId AND state = 'Active'
            """, new { hold.HoldId, State = hold.State.ToString(), hold.CompletedAt }, ct));
        RequireSingleWrite(count, "Hold transition");
    }

    public async Task SaveConfirmationAsync(Booking booking, CancellationToken ct)
    {
        var count = await connection.ExecuteAsync(Command("""
            UPDATE bookings SET confirmed_hold_id = @ConfirmedHoldId, confirmed_at = @ConfirmedAt
            WHERE booking_id = @BookingId AND confirmed_hold_id IS NULL
            """, new { booking.BookingId, booking.ConfirmedHoldId, booking.ConfirmedAt }, ct));
        RequireSingleWrite(count, "Booking confirmation");
    }

    public Task<int> InsertOutboxAsync(BookingConfirmedMessage message, CancellationToken ct) =>
        connection.ExecuteAsync(Command("""
            INSERT INTO outbox (message_id, event_type, aggregate_id, payload, occurred_at, next_attempt_at)
            VALUES (@MessageId, 'BookingConfirmed', @BookingId, @Payload, @ConfirmedAt, @ConfirmedAt)
            """, new
        {
            message.MessageId,
            message.BookingId,
            message.ConfirmedAt,
            Payload = JsonSerializer.Serialize(message, OperationResult.JsonOptions)
        }, ct));

    public Task<int> AppendAuditAsync(CapacityHold hold, string transition, DateTimeOffset decisionTime,
        RequestContext context, string details, CancellationToken ct) =>
        connection.ExecuteAsync(Command("""
            INSERT INTO audit_transitions (booking_id, voyage_id, hold_id, transition, occurred_at, trace_id, details)
            VALUES (@BookingId, @VoyageId, @HoldId, @Transition, @DecisionTime, @TraceId, @Details)
            """, new
        {
            hold.BookingId,
            hold.VoyageId,
            hold.HoldId,
            Transition = transition,
            DecisionTime = decisionTime,
            TraceId = context.TraceId ?? string.Empty,
            Details = details
        }, ct));

    public Task<int> AppendCreateRejectionAsync(string bookingId, string voyageId, RequestContext context, string code, CancellationToken ct) =>
        connection.ExecuteAsync(Command("""
            INSERT INTO audit_transitions (booking_id, voyage_id, hold_id, transition, occurred_at, trace_id, details)
            VALUES (@BookingId, @VoyageId, NULL, 'CapacityHoldRejected', clock_timestamp(), @TraceId, @Code)
            """, new { BookingId = bookingId, VoyageId = voyageId, TraceId = context.TraceId ?? string.Empty, Code = code }, ct));

    public async Task<DateTimeOffset> ReadClockAsync(CancellationToken ct) =>
        Utc(await connection.QuerySingleAsync<DateTime>(Command("SELECT clock_timestamp()", null, ct)));

    public async Task<HoldSnapshot?> ReadOwnedHoldAsync(Guid holdId, string customerId, CancellationToken ct)
    {
        var row = await connection.QuerySingleOrDefaultAsync<HoldRow>(Command("""
            SELECT h.hold_id, h.booking_id, h.voyage_id, h.quantity, h.state,
                   h.created_at, h.expires_at, h.completed_at, clock_timestamp() AS database_now
            FROM capacity_holds h JOIN bookings b ON b.booking_id = h.booking_id
            WHERE h.hold_id = @HoldId AND b.customer_id = @CustomerId
            """, new { HoldId = holdId, CustomerId = customerId }, ct));
        return row is null ? null : new HoldSnapshot(row.ToDomain(), Utc(row.DatabaseNow));
    }

    public Task<int> SetLocalLockTimeoutAsync(TimeSpan timeout, CancellationToken ct) =>
        connection.ExecuteAsync(Command("SELECT set_config('lock_timeout', @Timeout, true)",
            new { Timeout = Math.Ceiling(timeout.TotalMilliseconds).ToString(CultureInfo.InvariantCulture) + "ms" }, ct));

    public async Task<IReadOnlyList<ExpiryCandidate>> ReadDuePageAsync(ExpirySweepState state, int pageSize, CancellationToken ct)
    {
        // This is discovery, not a hold-first claim. The existing partial due index
        // supports the keyset and a fixed cutoff makes each sweep a finite traversal.
        var sql = """
            SELECT hold_id, booking_id, voyage_id, expires_at
            FROM capacity_holds
            WHERE state = 'Active' AND expires_at <= @Cutoff
            """;
        if (state.LastExpiresAt.HasValue)
            sql += " AND (expires_at, hold_id) > (@LastExpiresAt, @LastHoldId)";
        sql += " ORDER BY expires_at, hold_id LIMIT @PageSize";
        var rows = await connection.QueryAsync<ExpiryCandidateRow>(Command(sql,
            new { state.Cutoff, state.LastExpiresAt, state.LastHoldId, PageSize = pageSize }, ct));
        return rows.Select(row => new ExpiryCandidate(row.HoldId, row.BookingId, row.VoyageId, Utc(row.ExpiresAt))).ToArray();
    }

    private CommandDefinition Command(string sql, object? parameters, CancellationToken ct) =>
        new(sql, parameters, transaction, cancellationToken: ct);
    private static DateTimeOffset Utc(DateTime value) => new(DateTime.SpecifyKind(value, DateTimeKind.Utc));
    private static void RequireSingleWrite(int affected, string operation)
    {
        if (affected != 1)
            throw new InvalidOperationException($"{operation} must affect exactly one locked row.");
    }

    internal sealed class HoldIdentity
    {
        public string BookingId { get; set; } = "";
        public string VoyageId { get; set; } = "";
    }
    internal sealed record HoldSnapshot(CapacityHold Hold, DateTimeOffset DatabaseNow);
    internal sealed record ExpiryCandidate(Guid HoldId, string BookingId, string VoyageId, DateTimeOffset ExpiresAt);
    private sealed class CapacityRow
    {
        public string VoyageId { get; set; } = "";
        public int Total { get; set; }
        public int Reserved { get; set; }
        public int Confirmed { get; set; }
        public bool IsOpen { get; set; }
    }
    private sealed class BookingRow
    {
        public string BookingId { get; set; } = "";
        public string VoyageId { get; set; } = "";
        public string CustomerId { get; set; } = "";
        public int Quantity { get; set; }
        public Guid? ConfirmedHoldId { get; set; }
        public DateTime? ConfirmedAt { get; set; }
    }
    private sealed class HoldRow
    {
        public Guid HoldId { get; set; }
        public string BookingId { get; set; } = "";
        public string VoyageId { get; set; } = "";
        public int Quantity { get; set; }
        public string State { get; set; } = "";
        public DateTime CreatedAt { get; set; }
        public DateTime ExpiresAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public DateTime DatabaseNow { get; set; }
        public CapacityHold ToDomain() => new(HoldId, BookingId, VoyageId, new CapacityUnits(Quantity),
            Enum.Parse<HoldState>(State), Utc(CreatedAt), Utc(ExpiresAt), CompletedAt is { } at ? Utc(at) : null);
    }
    private sealed class ExpiryCandidateRow
    {
        public Guid HoldId { get; set; }
        public string BookingId { get; set; } = "";
        public string VoyageId { get; set; } = "";
        public DateTime ExpiresAt { get; set; }
    }
}
