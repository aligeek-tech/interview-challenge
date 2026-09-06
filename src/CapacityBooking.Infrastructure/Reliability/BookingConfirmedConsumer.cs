using System.Data;
using CapacityBooking.Application;
using Dapper;
using Microsoft.Extensions.Logging;

namespace CapacityBooking.Infrastructure.Reliability;

/// <summary>The inbox receipt and this consumer's database effect share one transaction.</summary>
public sealed class BookingConfirmedConsumer(
    Database database,
    IExecutionObserver observer,
    ILogger<BookingConfirmedConsumer> logger) : IBookingConfirmedConsumer
{
    public const string ConsumerName = "booking-confirmation-projection-v1";

    public async Task<bool> ConsumeAsync(BookingConfirmedMessage message, CancellationToken ct = default)
    {
        BookingConfirmedValidation.Validate(message);

        await using var connection = await database.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        // A concurrent duplicate waits on this unique key until the first transaction resolves.
        var inserted = await connection.ExecuteAsync(new CommandDefinition("""
            INSERT INTO inbox (consumer_name, message_id, processed_at)
            VALUES (@ConsumerName, @MessageId, clock_timestamp())
            ON CONFLICT (consumer_name, message_id) DO NOTHING
            """, new { ConsumerName, message.MessageId }, transaction, cancellationToken: ct));

        if (inserted == 0)
        {
            await transaction.CommitAsync(ct);
            logger.LogInformation(
                "Duplicate BookingConfirmed delivery acknowledged: MessageId {MessageId}, BookingId {BookingId}, VoyageId {VoyageId}, HoldId {HoldId}, TraceId {TraceId}",
                message.MessageId, message.BookingId, message.VoyageId, message.HoldId, message.TraceId);
            return false;
        }

        await connection.ExecuteAsync(new CommandDefinition("""
            INSERT INTO booking_confirmations
                (booking_id, voyage_id, hold_id, quantity, message_id, confirmed_at)
            VALUES (@BookingId, @VoyageId, @HoldId, @Quantity, @MessageId, @ConfirmedAt)
            """, message, transaction, cancellationToken: ct));

        await observer.ReachedAsync("consumer.before-commit", message.MessageId.ToString("D"), ct);
        await transaction.CommitAsync(ct);
        logger.LogInformation(
            "BookingConfirmed projection committed: MessageId {MessageId}, BookingId {BookingId}, VoyageId {VoyageId}, HoldId {HoldId}, TraceId {TraceId}",
            message.MessageId, message.BookingId, message.VoyageId, message.HoldId, message.TraceId);
        return true;
    }
}
