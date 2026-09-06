using System.Data;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CapacityBooking.Application;
using CapacityBooking.Domain;
using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CapacityBooking.Infrastructure;

/// <summary>
/// Application transaction coordinator for the capacity and booking aggregates.
/// All writers lock voyage, booking, then hold; persisted idempotency precedes them.
/// </summary>
public sealed class BookingService : IBookingService
{
    private readonly Database _database;
    private readonly IExecutionObserver _observer;
    private readonly ILogger<BookingService> _logger;
    private readonly TimeSpan _holdTtl;

    public BookingService(Database database, IOptions<BookingOptions> options,
        IExecutionObserver observer, ILogger<BookingService> logger)
    {
        _database = database;
        _observer = observer;
        _logger = logger;
        _holdTtl = options.Value.HoldTtl;
        if (_holdTtl <= TimeSpan.Zero || _holdTtl > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(options), "Hold TTL must be positive and at most one day.");
    }

    public Task<OperationResult> CreateHoldAsync(string voyageId, CreateHoldRequest request,
        string idempotencyKey, RequestContext context, CancellationToken ct = default)
    {
        var invalid = ValidateContext(context) ?? ValidateKey(idempotencyKey);
        if (invalid is not null)
            return Task.FromResult(invalid);
        if (!IsIdentifier(voyageId) || request is null || !IsIdentifier(request.BookingId) || request.Quantity <= 0)
            return Task.FromResult(Error(400, "INVALID_REQUEST", "VoyageId and BookingId must be bounded identifiers and Quantity must be positive."));

        var operation = $"create-hold:{voyageId}";
        var fingerprint = Fingerprint(new { Operation = "CreateHold", VoyageId = voyageId,
            context.CustomerId, request.BookingId, request.Quantity });

        return ExecuteIdempotentAsync(operation, idempotencyKey, fingerprint, context,
            async (connection, transaction) =>
            {
                var capacity = await LockCapacityAsync(connection, transaction, voyageId, ct);
                if (capacity is null)
                    return await RejectCreateAsync(connection, transaction, request.BookingId, voyageId,
                        context, 404, "VOYAGE_NOT_FOUND", "Voyage was not found.", ct);
                if (!capacity.IsOpen)
                    return await RejectCreateAsync(connection, transaction, request.BookingId, voyageId,
                        context, 409, "VOYAGE_CLOSED", "The voyage is closed to new holds.", ct);

                var booking = await LockBookingAsync(connection, transaction, request.BookingId, ct);
                if (booking is null)
                {
                    // A rejected request must not create and permanently bind a draft booking.
                    if (request.Quantity > capacity.Available)
                        return await RejectCreateAsync(connection, transaction, request.BookingId, voyageId,
                            context, 409, "CAPACITY_UNAVAILABLE", "The voyage has insufficient available capacity.", ct);

                    // A different voyage may concurrently attempt the same global BookingId.
                    // Its uniqueness wait is completed before the decision clock is sampled.
                    await connection.ExecuteAsync(Command("""
                        INSERT INTO bookings (booking_id, voyage_id, customer_id, quantity)
                        VALUES (@BookingId, @VoyageId, @CustomerId, @Quantity)
                        ON CONFLICT (booking_id) DO NOTHING
                        """, new { request.BookingId, VoyageId = voyageId, context.CustomerId, request.Quantity }, transaction, ct));
                    booking = await LockBookingAsync(connection, transaction, request.BookingId, ct)
                        ?? throw new InvalidOperationException("Booking uniqueness claim was not visible.");
                }

                if (!booking.MatchesRequest(context.CustomerId, voyageId, request.Quantity))
                    return await RejectCreateAsync(connection, transaction, request.BookingId, voyageId,
                        context, 409, "BOOKING_REQUEST_CONFLICT", "BookingId is already bound to another logical request.", ct);

                var priorRow = await connection.QuerySingleOrDefaultAsync<HoldRow>(Command("""
                    SELECT hold_id, booking_id, voyage_id, quantity, state, created_at, expires_at, completed_at
                    FROM capacity_holds
                    WHERE booking_id = @BookingId AND voyage_id = @VoyageId AND state = 'Active'
                    FOR UPDATE
                    """, new { request.BookingId, VoyageId = voyageId }, transaction, ct));
                await _observer.ReachedAsync("business.after-locks", voyageId, ct);
                var decisionTime = await ReadClockAsync(connection, transaction, ct);

                if (booking.IsConfirmed)
                    return await RejectCreateAsync(connection, transaction, request.BookingId, voyageId,
                        context, 409, "BOOKING_ALREADY_CONFIRMED", "This logical booking request is already confirmed.", ct);

                if (priorRow is not null)
                {
                    var prior = priorRow.ToDomain();
                    if (!capacity.ExpireHold(prior, decisionTime))
                        return await RejectCreateAsync(connection, transaction, request.BookingId, voyageId,
                            context, 409, "HOLD_ALREADY_ACTIVE", "This booking already has an active hold; its deadline is unchanged.", ct);

                    await SaveCapacityAsync(connection, transaction, capacity, ct);
                    await SaveTransitionAsync(connection, transaction, prior, ct);
                    await AuditAsync(connection, transaction, prior, "CapacityHoldExpired", decisionTime,
                        context, "Expired hold released before a replacement request.", ct);
                }

                if (request.Quantity > capacity.Available)
                    return await RejectCreateAsync(connection, transaction, request.BookingId, voyageId,
                        context, 409, "CAPACITY_UNAVAILABLE", "The voyage has insufficient available capacity.", ct);

                var hold = capacity.CreateHold(Guid.NewGuid(), booking, decisionTime, _holdTtl);
                await SaveCapacityAsync(connection, transaction, capacity, ct);
                await connection.ExecuteAsync(Command("""
                    INSERT INTO capacity_holds
                        (hold_id, booking_id, voyage_id, quantity, state, created_at, expires_at)
                    VALUES (@HoldId, @BookingId, @VoyageId, @Quantity, 'Active', @CreatedAt, @ExpiresAt)
                    """, new { hold.HoldId, hold.BookingId, hold.VoyageId, Quantity = hold.Quantity.Value,
                        hold.CreatedAt, ExpiresAt = hold.Deadline.ExpiresAt }, transaction, ct));
                await AuditAsync(connection, transaction, hold, "CapacityHoldCreated", decisionTime,
                    context, "Capacity temporarily reserved.", ct);
                _logger.LogInformation("CapacityHoldCreated decision: BookingId {BookingId}, VoyageId {VoyageId}, HoldId {HoldId}, Quantity {Quantity}, ExpiresAt {ExpiresAt}",
                    hold.BookingId, hold.VoyageId, hold.HoldId, hold.Quantity.Value, hold.Deadline.ExpiresAt);
                return OperationResult.From(201, ToView(hold));
            }, ct);
    }

    public Task<OperationResult> ConfirmAsync(Guid holdId, string idempotencyKey,
        RequestContext context, CancellationToken ct = default)
    {
        var invalid = ValidateContext(context) ?? ValidateKey(idempotencyKey);
        if (invalid is not null)
            return Task.FromResult(invalid);

        var operation = $"confirm-hold:{holdId:D}";
        var fingerprint = Fingerprint(new { Operation = "ConfirmHold", HoldId = holdId, context.CustomerId });
        return ExecuteIdempotentAsync(operation, idempotencyKey, fingerprint, context,
            async (connection, transaction) =>
            {
                var locked = await LockOwnedHoldAsync(connection, transaction, holdId, context.CustomerId, ct);
                if (locked is null)
                    return Error(404, "HOLD_NOT_FOUND", "Hold was not found.");
                var (capacity, booking, hold, decisionTime) = locked;

                _logger.LogInformation("Confirm/expiry decision: BookingId {BookingId}, VoyageId {VoyageId}, HoldId {HoldId}, State {State}, DecisionTime {DecisionTime}, ExpiresAt {ExpiresAt}",
                    hold.BookingId, hold.VoyageId, hold.HoldId, hold.State, decisionTime, hold.Deadline.ExpiresAt);

                if (booking.IsConfirmed)
                {
                    if (booking.ConfirmedHoldId == holdId && hold.State == HoldState.Consumed)
                        return OperationResult.From(200, ToConfirmation(booking));
                    return Error(409, "BOOKING_ALREADY_CONFIRMED", "This logical booking request is already confirmed using another hold.");
                }

                if (hold.State != HoldState.Active)
                    return Error(409, "HOLD_NOT_ACTIVE", "An expired, cancelled or consumed hold cannot confirm a booking.");
                if (hold.Deadline.IsExpiredAt(decisionTime))
                {
                    capacity.ExpireHold(hold, decisionTime);
                    await SaveCapacityAsync(connection, transaction, capacity, ct);
                    await SaveTransitionAsync(connection, transaction, hold, ct);
                    await AuditAsync(connection, transaction, hold, "CapacityHoldExpired", decisionTime,
                        context, "Confirmation acquired locks at or after the deadline; expiry won.", ct);
                    return Error(409, "HOLD_EXPIRED", "The hold deadline passed before confirmation acquired its business locks.");
                }

                capacity.ConsumeHold(hold, decisionTime);
                booking.Confirm(hold, decisionTime);
                await SaveCapacityAsync(connection, transaction, capacity, ct);
                await SaveTransitionAsync(connection, transaction, hold, ct);
                var bookingUpdated = await connection.ExecuteAsync(Command("""
                    UPDATE bookings SET confirmed_hold_id = @HoldId, confirmed_at = @DecisionTime
                    WHERE booking_id = @BookingId AND confirmed_hold_id IS NULL
                    """, new { hold.HoldId, hold.BookingId, DecisionTime = decisionTime }, transaction, ct));
                RequireSingleWrite(bookingUpdated, "Booking confirmation");
                await _observer.ReachedAsync("confirm.after-decision", holdId.ToString("D"), ct);

                var message = new BookingConfirmedMessage(Guid.NewGuid(), booking.BookingId, booking.VoyageId,
                    holdId, booking.Quantity.Value, decisionTime, context.TraceId ?? string.Empty);
                await connection.ExecuteAsync(Command("""
                    INSERT INTO outbox (message_id, event_type, aggregate_id, payload, occurred_at, next_attempt_at)
                    VALUES (@MessageId, 'BookingConfirmed', @BookingId, @Payload, @ConfirmedAt, @ConfirmedAt)
                    """, new { message.MessageId, message.BookingId, message.ConfirmedAt,
                        Payload = JsonSerializer.Serialize(message, OperationResult.JsonOptions) }, transaction, ct));
                await AuditAsync(connection, transaction, hold, "CapacityHoldConsumed", decisionTime,
                    context, "Reserved capacity transferred to confirmed capacity.", ct);
                await AuditAsync(connection, transaction, hold, "BookingConfirmed", decisionTime,
                    context, $"Integration message {message.MessageId:D} committed in the same transaction.", ct);
                await _observer.ReachedAsync("confirm.before-commit", holdId.ToString("D"), ct);
                return OperationResult.From(200, ToConfirmation(booking));
            }, ct);
    }

    public async Task<OperationResult> CancelAsync(Guid holdId, RequestContext context,
        CancellationToken ct = default)
    {
        var invalid = ValidateContext(context);
        if (invalid is not null)
            return invalid;
        using var scope = _logger.BeginScope(new Dictionary<string, object>
            { ["TraceId"] = context.TraceId ?? string.Empty, ["HoldId"] = holdId });
        await using var connection = await _database.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var locked = await LockOwnedHoldAsync(connection, transaction, holdId, context.CustomerId, ct);
        if (locked is null)
        {
            await transaction.CommitAsync(ct);
            return Error(404, "HOLD_NOT_FOUND", "Hold was not found.");
        }
        var (capacity, _, hold, decisionTime) = locked;
        if (hold.State == HoldState.Consumed)
        {
            await transaction.CommitAsync(ct);
            return Error(409, "HOLD_CONSUMED", "A consumed hold cannot be cancelled.");
        }
        if (capacity.CancelHold(hold, decisionTime))
        {
            await SaveCapacityAsync(connection, transaction, capacity, ct);
            await SaveTransitionAsync(connection, transaction, hold, ct);
            await AuditAsync(connection, transaction, hold,
                hold.State == HoldState.Expired ? "CapacityHoldExpired" : "CapacityHoldCancelled",
                decisionTime, context, "Capacity returned exactly once during cancellation.", ct);
        }
        await transaction.CommitAsync(ct);
        _logger.LogInformation("Cancellation completed: BookingId {BookingId}, VoyageId {VoyageId}, HoldId {HoldId}, State {State}",
            hold.BookingId, hold.VoyageId, holdId, hold.State);
        return OperationResult.From(200, ToView(hold));
    }

    public async Task<OperationResult> GetAsync(Guid holdId, RequestContext context,
        CancellationToken ct = default)
    {
        var invalid = ValidateContext(context);
        if (invalid is not null)
            return invalid;
        await using var connection = await _database.OpenAsync(ct);
        var row = await connection.QuerySingleOrDefaultAsync<HoldRow>(Command("""
            SELECT h.hold_id, h.booking_id, h.voyage_id, h.quantity, h.state,
                   h.created_at, h.expires_at, h.completed_at, clock_timestamp() AS database_now
            FROM capacity_holds h JOIN bookings b ON b.booking_id = h.booking_id
            WHERE h.hold_id = @HoldId AND b.customer_id = @CustomerId
            """, new { HoldId = holdId, context.CustomerId }, null, ct));
        if (row is null)
            return Error(404, "HOLD_NOT_FOUND", "Hold was not found.");
        var hold = row.ToDomain();
        var view = ToView(hold);
        if (hold.State == HoldState.Active && hold.Deadline.IsExpiredAt(Utc(row.DatabaseNow)))
            view = view with { State = "Expired" };
        return OperationResult.From(200, view);
    }

    private async Task<OperationResult> ExecuteIdempotentAsync(string operation, string key,
        string fingerprint, RequestContext context,
        Func<NpgsqlConnection, NpgsqlTransaction, Task<OperationResult>> execute, CancellationToken ct)
    {
        using var scope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["TraceId"] = context.TraceId ?? string.Empty,
            ["Operation"] = operation,
            ["IdempotencyKeyHash"] = Fingerprint(key)[..16]
        });
        await using var connection = await _database.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var identity = new { context.CustomerId, Operation = operation, IdempotencyKey = key, Fingerprint = fingerprint };
        try
        {
            var claimed = await connection.ExecuteAsync(Command("""
                INSERT INTO idempotency_records (customer_id, operation, idempotency_key, fingerprint)
                VALUES (@CustomerId, @Operation, @IdempotencyKey, @Fingerprint)
                ON CONFLICT (customer_id, operation, idempotency_key) DO NOTHING
                """, identity, transaction, ct));
            if (claimed == 0)
            {
                // READ COMMITTED takes a fresh snapshot after the unique-index wait.
                var existing = await connection.QuerySingleAsync<IdempotencyRow>(Command("""
                    SELECT fingerprint, status_code, response_json
                    FROM idempotency_records
                    WHERE customer_id = @CustomerId AND operation = @Operation AND idempotency_key = @IdempotencyKey
                    """, identity, transaction, ct));
                if (existing.Fingerprint != fingerprint)
                {
                    await transaction.CommitAsync(ct);
                    _logger.LogWarning("IdempotencyConflict: the same scoped key was used with a different request fingerprint.");
                    return Error(409, "IDEMPOTENCY_CONFLICT", "This idempotency key was already used for a different request.");
                }
                if (existing.StatusCode is null || existing.ResponseJson is null)
                    throw new InvalidOperationException("An incomplete idempotency record became visible.");
                await transaction.CommitAsync(ct);
                _logger.LogInformation("DuplicateCommandDetected: replaying persisted HTTP status {StatusCode}", existing.StatusCode);
                return new OperationResult(existing.StatusCode.Value, existing.ResponseJson, true);
            }

            var result = await execute(connection, transaction);
            var completed = await connection.ExecuteAsync(Command("""
                UPDATE idempotency_records SET status_code = @StatusCode, response_json = @Json
                WHERE customer_id = @CustomerId AND operation = @Operation AND idempotency_key = @IdempotencyKey
                """, new { context.CustomerId, Operation = operation, IdempotencyKey = key, result.StatusCode, result.Json }, transaction, ct));
            RequireSingleWrite(completed, "Idempotency completion");
            await transaction.CommitAsync(ct);
            _logger.LogInformation("Business operation committed with HTTP status {StatusCode}", result.StatusCode);
            return result;
        }
        catch (PostgresException exception) when (exception.SqlState is "40P01" or "40001" or "55P03")
        {
            // Disposal rolls back both the claim and business writes. The caller may retry the same key.
            _logger.LogWarning("Database concurrency conflict {SqlState}; the whole operation remains retryable", exception.SqlState);
            throw;
        }
    }

    private async Task<LockedHold?> LockOwnedHoldAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, Guid holdId, string customerId, CancellationToken ct)
    {
        // Immutable identity lookup is not a lock acquisition. Never lock the hold first.
        var identity = await connection.QuerySingleOrDefaultAsync<HoldIdentity>(Command("""
            SELECT h.booking_id, h.voyage_id
            FROM capacity_holds h JOIN bookings b ON b.booking_id = h.booking_id
            WHERE h.hold_id = @HoldId AND b.customer_id = @CustomerId
            """, new { HoldId = holdId, CustomerId = customerId }, transaction, ct));
        if (identity is null)
            return null;

        var capacity = await LockCapacityAsync(connection, transaction, identity.VoyageId, ct)
            ?? throw new InvalidOperationException("The hold's capacity root is missing.");
        var booking = await LockBookingAsync(connection, transaction, identity.BookingId, ct)
            ?? throw new InvalidOperationException("The hold's booking root is missing.");
        var row = await connection.QuerySingleAsync<HoldRow>(Command("""
            SELECT hold_id, booking_id, voyage_id, quantity, state, created_at, expires_at, completed_at
            FROM capacity_holds WHERE hold_id = @HoldId FOR UPDATE
            """, new { HoldId = holdId }, transaction, ct));
        if (booking.CustomerId != customerId)
            return null;
        await _observer.ReachedAsync("business.after-locks", holdId.ToString("D"), ct);
        var decisionTime = await ReadClockAsync(connection, transaction, ct);
        return new LockedHold(capacity, booking, row.ToDomain(), decisionTime);
    }

    private static async Task<VoyageCapacity?> LockCapacityAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, string voyageId, CancellationToken ct)
    {
        var row = await connection.QuerySingleOrDefaultAsync<CapacityRow>(Command("""
            SELECT voyage_id, total, reserved, confirmed, is_open
            FROM voyage_capacity WHERE voyage_id = @VoyageId FOR UPDATE
            """, new { VoyageId = voyageId }, transaction, ct));
        return row is null ? null : new VoyageCapacity(row.VoyageId, row.Total, row.Reserved, row.Confirmed, row.IsOpen);
    }

    private static async Task<Booking?> LockBookingAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, string bookingId, CancellationToken ct)
    {
        var row = await connection.QuerySingleOrDefaultAsync<BookingRow>(Command("""
            SELECT booking_id, voyage_id, customer_id, quantity, confirmed_hold_id, confirmed_at
            FROM bookings WHERE booking_id = @BookingId FOR UPDATE
            """, new { BookingId = bookingId }, transaction, ct));
        return row is null ? null : new Booking(row.BookingId, row.VoyageId, row.CustomerId,
            new CapacityUnits(row.Quantity), row.ConfirmedHoldId, row.ConfirmedAt is { } at ? Utc(at) : null);
    }

    private static async Task SaveCapacityAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        VoyageCapacity capacity, CancellationToken ct)
    {
        var count = await connection.ExecuteAsync(Command("""
            UPDATE voyage_capacity SET reserved = @Reserved, confirmed = @Confirmed
            WHERE voyage_id = @VoyageId
            """, new { capacity.VoyageId, capacity.Reserved, capacity.Confirmed }, transaction, ct));
        RequireSingleWrite(count, "Capacity mutation");
    }

    private static async Task SaveTransitionAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        CapacityHold hold, CancellationToken ct)
    {
        var count = await connection.ExecuteAsync(Command("""
            UPDATE capacity_holds SET state = @State, completed_at = @CompletedAt
            WHERE hold_id = @HoldId AND state = 'Active'
            """, new { hold.HoldId, State = hold.State.ToString(), hold.CompletedAt }, transaction, ct));
        RequireSingleWrite(count, "Hold transition");
    }

    private async Task<OperationResult> RejectCreateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string bookingId, string voyageId, RequestContext context, int status, string code, string message, CancellationToken ct)
    {
        await connection.ExecuteAsync(Command("""
            INSERT INTO audit_transitions (booking_id, voyage_id, hold_id, transition, occurred_at, trace_id, details)
            VALUES (@BookingId, @VoyageId, NULL, 'CapacityHoldRejected', clock_timestamp(), @TraceId, @Code)
            """, new { BookingId = bookingId, VoyageId = voyageId, TraceId = context.TraceId ?? string.Empty, Code = code }, transaction, ct));
        _logger.LogInformation("CapacityHoldRejected decision: BookingId {BookingId}, VoyageId {VoyageId}, Reason {Reason}",
            bookingId, voyageId, code);
        return Error(status, code, message);
    }

    private static Task<int> AuditAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        CapacityHold hold, string transition, DateTimeOffset decisionTime, RequestContext context, string details, CancellationToken ct) =>
        connection.ExecuteAsync(Command("""
            INSERT INTO audit_transitions (booking_id, voyage_id, hold_id, transition, occurred_at, trace_id, details)
            VALUES (@BookingId, @VoyageId, @HoldId, @Transition, @DecisionTime, @TraceId, @Details)
            """, new { hold.BookingId, hold.VoyageId, hold.HoldId, Transition = transition,
                DecisionTime = decisionTime, TraceId = context.TraceId ?? string.Empty, Details = details }, transaction, ct));

    private static async Task<DateTimeOffset> ReadClockAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, CancellationToken ct) =>
        Utc(await connection.QuerySingleAsync<DateTime>(Command("SELECT clock_timestamp()", null, transaction, ct)));

    private static HoldView ToView(CapacityHold hold) => new(hold.HoldId, hold.BookingId, hold.VoyageId,
        hold.Quantity.Value, hold.State.ToString(), hold.CreatedAt, hold.Deadline.ExpiresAt, hold.CompletedAt);

    private static ConfirmationView ToConfirmation(Booking booking) => new(booking.BookingId, booking.VoyageId,
        booking.ConfirmedHoldId ?? throw new InvalidOperationException("Booking is not confirmed."),
        booking.Quantity.Value, booking.ConfirmedAt ?? throw new InvalidOperationException("Booking is not confirmed."));

    private static OperationResult Error(int statusCode, string code, string message) =>
        OperationResult.From(statusCode, new ApiError(code, message));
    private static OperationResult? ValidateContext(RequestContext context) =>
        context is null || !IsIdentifier(context.CustomerId)
            ? Error(401, "CUSTOMER_REQUIRED", "A valid customer identity is required.") : null;
    private static OperationResult? ValidateKey(string key) =>
        !IsIdentifier(key) ? Error(400, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key must contain 1 to 128 non-control characters.") : null;
    private static bool IsIdentifier(string? value) =>
        !string.IsNullOrWhiteSpace(value) && value.Length <= 128 && !value.Any(char.IsControl);
    private static string Fingerprint<T>(T value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value, OperationResult.JsonOptions))));
    private static DateTimeOffset Utc(DateTime value) => new(DateTime.SpecifyKind(value, DateTimeKind.Utc));
    private static CommandDefinition Command(string sql, object? parameters, NpgsqlTransaction? transaction,
        CancellationToken ct) => new(sql, parameters, transaction, cancellationToken: ct);
    private static void RequireSingleWrite(int affected, string operation)
    {
        if (affected != 1)
            throw new InvalidOperationException($"{operation} must affect exactly one locked row.");
    }

    private sealed record LockedHold(VoyageCapacity Capacity, Booking Booking, CapacityHold Hold, DateTimeOffset DecisionTime);
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
        public DateTime CreatedAt { get; set; }
        public DateTime ExpiresAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public DateTime DatabaseNow { get; set; }
        public CapacityHold ToDomain() => new(HoldId, BookingId, VoyageId, new CapacityUnits(Quantity),
            Enum.Parse<HoldState>(State), Utc(CreatedAt), Utc(ExpiresAt), CompletedAt is { } at ? Utc(at) : null);
    }
    private sealed class IdempotencyRow
    {
        public string Fingerprint { get; set; } = "";
        public int? StatusCode { get; set; }
        public string? ResponseJson { get; set; }
    }
}
