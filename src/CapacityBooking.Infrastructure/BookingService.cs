using System.Data;
using CapacityBooking.Application;
using CapacityBooking.Domain;
using CapacityBooking.Infrastructure.Business;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using static CapacityBooking.Infrastructure.Business.BookingResponses;

namespace CapacityBooking.Infrastructure;

/// <summary>Coordinates use cases; persistence, idempotency and response projection have focused collaborators.</summary>
public sealed class BookingService : IBookingService
{
    private readonly Database _database;
    private readonly IExecutionObserver _observer;
    private readonly ILogger<BookingService> _logger;
    private readonly TimeSpan _holdTtl;
    private readonly IdempotentCommandExecutor _commands;

    public BookingService(Database database, IOptions<BookingOptions> options,
        IExecutionObserver observer, ILogger<BookingService> logger)
    {
        _database = database;
        _observer = observer;
        _logger = logger;
        _holdTtl = options.Value.HoldTtl;
        _commands = new IdempotentCommandExecutor(database, logger);
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
        var fingerprint = IdempotentCommandExecutor.Fingerprint(new
        {
            Operation = "CreateHold",
            VoyageId = voyageId,
            context.CustomerId,
            request.BookingId,
            request.Quantity
        });
        return _commands.ExecuteAsync(operation, idempotencyKey, fingerprint, context,
            async session =>
            {
                var capacity = await session.LockCapacityAsync(voyageId, ct);
                if (capacity is null)
                    return await RejectCreateAsync(session, request.BookingId, voyageId,
                        context, 404, "VOYAGE_NOT_FOUND", "Voyage was not found.", ct);
                if (!capacity.IsOpen)
                    return await RejectCreateAsync(session, request.BookingId, voyageId,
                        context, 409, "VOYAGE_CLOSED", "The voyage is closed to new holds.", ct);

                var booking = await session.LockBookingAsync(request.BookingId, ct);
                if (booking is null)
                {
                    if (request.Quantity > capacity.Available)
                        return await RejectCreateAsync(session, request.BookingId, voyageId,
                            context, 409, "CAPACITY_UNAVAILABLE", "The voyage has insufficient available capacity.", ct);
                    // The uniqueness wait for a global BookingId must precede the decision clock.
                    await session.InsertBookingIfAbsentAsync(request.BookingId, voyageId, context.CustomerId, request.Quantity, ct);
                    booking = await session.LockBookingAsync(request.BookingId, ct)
                        ?? throw new InvalidOperationException("Booking uniqueness claim was not visible.");
                }
                if (!booking.MatchesRequest(context.CustomerId, voyageId, request.Quantity))
                    return await RejectCreateAsync(session, request.BookingId, voyageId,
                        context, 409, "BOOKING_REQUEST_CONFLICT", "BookingId is already bound to another logical request.", ct);

                var prior = await session.LockActiveHoldAsync(request.BookingId, voyageId, ct);
                await _observer.ReachedAsync("business.after-locks", voyageId, ct);
                var decisionTime = await session.ReadClockAsync(ct);
                if (booking.IsConfirmed)
                    return await RejectCreateAsync(session, request.BookingId, voyageId,
                        context, 409, "BOOKING_ALREADY_CONFIRMED", "This logical booking request is already confirmed.", ct);

                if (prior is not null && !await HoldExpiryTransition.ApplyAsync(session, capacity, prior,
                    decisionTime, context, "Expired hold released before a replacement request.", ct))
                    return await RejectCreateAsync(session, request.BookingId, voyageId,
                        context, 409, "HOLD_ALREADY_ACTIVE", "This booking already has an active hold; its deadline is unchanged.", ct);
                if (request.Quantity > capacity.Available)
                    return await RejectCreateAsync(session, request.BookingId, voyageId,
                        context, 409, "CAPACITY_UNAVAILABLE", "The voyage has insufficient available capacity.", ct);

                var hold = capacity.CreateHold(Guid.NewGuid(), booking, decisionTime, _holdTtl);
                await session.SaveCapacityAsync(capacity, ct);
                await session.InsertHoldAsync(hold, ct);
                await session.AppendAuditAsync(hold, "CapacityHoldCreated", decisionTime, context, "Capacity temporarily reserved.", ct);
                _logger.LogInformation("CapacityHoldCreated decision: BookingId {BookingId}, VoyageId {VoyageId}, HoldId {HoldId}, Quantity {Quantity}, ExpiresAt {ExpiresAt}",
                    hold.BookingId, hold.VoyageId, hold.HoldId, hold.Quantity.Value, hold.Deadline.ExpiresAt);
                return OperationResult.From(201, Hold(hold));
            }, ct);
    }

    public Task<OperationResult> ConfirmAsync(Guid holdId, string idempotencyKey,
        RequestContext context, CancellationToken ct = default)
    {
        var invalid = ValidateContext(context) ?? ValidateKey(idempotencyKey);
        if (invalid is not null)
            return Task.FromResult(invalid);
        var operation = $"confirm-hold:{holdId:D}";
        var fingerprint = IdempotentCommandExecutor.Fingerprint(new { Operation = "ConfirmHold", HoldId = holdId, context.CustomerId });
        return _commands.ExecuteAsync(operation, idempotencyKey, fingerprint, context,
            async session =>
            {
                var locked = await LockOwnedHoldAsync(session, holdId, context.CustomerId, ct);
                if (locked is null)
                    return Error(404, "HOLD_NOT_FOUND", "Hold was not found.");
                var (capacity, booking, hold, decisionTime) = locked;
                _logger.LogInformation("Confirm/expiry decision: BookingId {BookingId}, VoyageId {VoyageId}, HoldId {HoldId}, State {State}, DecisionTime {DecisionTime}, ExpiresAt {ExpiresAt}",
                    hold.BookingId, hold.VoyageId, hold.HoldId, hold.State, decisionTime, hold.Deadline.ExpiresAt);

                if (booking.IsConfirmed)
                {
                    if (booking.ConfirmedHoldId == holdId && hold.State == HoldState.Consumed)
                        return OperationResult.From(200, Confirmation(booking));
                    return Error(409, "BOOKING_ALREADY_CONFIRMED", "This logical booking request is already confirmed using another hold.");
                }
                if (hold.State != HoldState.Active)
                    return Error(409, "HOLD_NOT_ACTIVE", "An expired, cancelled or consumed hold cannot confirm a booking.");
                if (await HoldExpiryTransition.ApplyAsync(session, capacity, hold, decisionTime, context,
                    "Confirmation acquired locks at or after the deadline; expiry won.", ct))
                    return Error(409, "HOLD_EXPIRED", "The hold deadline passed before confirmation acquired its business locks.");

                capacity.ConsumeHold(hold, decisionTime);
                booking.Confirm(hold, decisionTime);
                await session.SaveCapacityAsync(capacity, ct);
                await session.SaveTransitionAsync(hold, ct);
                await session.SaveConfirmationAsync(booking, ct);
                await _observer.ReachedAsync("confirm.after-decision", holdId.ToString("D"), ct);

                var message = new BookingConfirmedMessage(Guid.NewGuid(), booking.BookingId, booking.VoyageId,
                    holdId, booking.Quantity.Value, decisionTime, context.TraceId ?? string.Empty);
                await session.InsertOutboxAsync(message, ct);
                await session.AppendAuditAsync(hold, "CapacityHoldConsumed", decisionTime, context,
                    "Reserved capacity transferred to confirmed capacity.", ct);
                await session.AppendAuditAsync(hold, "BookingConfirmed", decisionTime, context,
                    $"Integration message {message.MessageId:D} committed in the same transaction.", ct);
                await _observer.ReachedAsync("confirm.before-commit", holdId.ToString("D"), ct);
                return OperationResult.From(200, Confirmation(booking));
            }, ct);
    }

    public async Task<OperationResult> CancelAsync(Guid holdId, RequestContext context, CancellationToken ct = default)
    {
        var invalid = ValidateContext(context);
        if (invalid is not null)
            return invalid;
        using var scope = _logger.BeginScope(new Dictionary<string, object>
        { ["TraceId"] = context.TraceId ?? string.Empty, ["HoldId"] = holdId });
        await using var connection = await _database.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var session = new BookingPersistenceSession(connection, transaction);
        var locked = await LockOwnedHoldAsync(session, holdId, context.CustomerId, ct);
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
        if (!await HoldExpiryTransition.ApplyAsync(session, capacity, hold, decisionTime, context,
                "Capacity returned exactly once during cancellation.", ct) &&
            capacity.CancelHold(hold, decisionTime))
        {
            await session.SaveCapacityAsync(capacity, ct);
            await session.SaveTransitionAsync(hold, ct);
            await session.AppendAuditAsync(hold, "CapacityHoldCancelled", decisionTime,
                context, "Capacity returned exactly once during cancellation.", ct);
        }
        await transaction.CommitAsync(ct);
        _logger.LogInformation("Cancellation completed: BookingId {BookingId}, VoyageId {VoyageId}, HoldId {HoldId}, State {State}",
            hold.BookingId, hold.VoyageId, holdId, hold.State);
        return OperationResult.From(200, Hold(hold));
    }

    public async Task<OperationResult> GetAsync(Guid holdId, RequestContext context, CancellationToken ct = default)
    {
        var invalid = ValidateContext(context);
        if (invalid is not null)
            return invalid;
        await using var connection = await _database.OpenAsync(ct);
        var snapshot = await new BookingPersistenceSession(connection).ReadOwnedHoldAsync(holdId, context.CustomerId, ct);
        if (snapshot is null)
            return Error(404, "HOLD_NOT_FOUND", "Hold was not found.");
        var view = Hold(snapshot.Hold);
        if (snapshot.Hold.State == HoldState.Active && snapshot.Hold.Deadline.IsExpiredAt(snapshot.DatabaseNow))
            view = view with { State = "Expired" };
        return OperationResult.From(200, view);
    }

    private async Task<LockedHold?> LockOwnedHoldAsync(BookingPersistenceSession session,
        Guid holdId, string customerId, CancellationToken ct)
    {
        var identity = await session.FindOwnedHoldIdentityAsync(holdId, customerId, ct);
        if (identity is null)
            return null;
        var capacity = await session.LockCapacityAsync(identity.VoyageId, ct)
            ?? throw new InvalidOperationException("The hold's capacity root is missing.");
        var booking = await session.LockBookingAsync(identity.BookingId, ct)
            ?? throw new InvalidOperationException("The hold's booking root is missing.");
        var hold = await session.LockHoldAsync(holdId, ct)
            ?? throw new InvalidOperationException("The hold is missing.");
        if (booking.CustomerId != customerId)
            return null;
        await _observer.ReachedAsync("business.after-locks", holdId.ToString("D"), ct);
        return new LockedHold(capacity, booking, hold, await session.ReadClockAsync(ct));
    }

    private async Task<OperationResult> RejectCreateAsync(BookingPersistenceSession session,
        string bookingId, string voyageId, RequestContext context, int status, string code, string message, CancellationToken ct)
    {
        await session.AppendCreateRejectionAsync(bookingId, voyageId, context, code, ct);
        _logger.LogInformation("CapacityHoldRejected decision: BookingId {BookingId}, VoyageId {VoyageId}, Reason {Reason}",
            bookingId, voyageId, code);
        return Error(status, code, message);
    }

    private static OperationResult? ValidateContext(RequestContext context) =>
        context is null || !IsIdentifier(context.CustomerId)
            ? Error(401, "CUSTOMER_REQUIRED", "A valid customer identity is required.") : null;
    private static OperationResult? ValidateKey(string key) =>
        !IsIdentifier(key) ? Error(400, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key must contain 1 to 128 non-control characters.") : null;
    private static bool IsIdentifier(string? value) =>
        !string.IsNullOrWhiteSpace(value) && value.Length <= 128 && !value.Any(char.IsControl);
    private sealed record LockedHold(VoyageCapacity Capacity, Booking Booking, CapacityHold Hold, DateTimeOffset DecisionTime);
}
