using System.Text.Json;
using CapacityBooking.Application;

namespace CapacityBooking.Infrastructure.Reliability;

public static class BookingConfirmedValidation
{
    public static BookingConfirmedMessage Read(string eventType, Guid messageId, string aggregateId, string payload)
    {
        if (eventType != "BookingConfirmed")
            throw Permanent("UNSUPPORTED_EVENT_TYPE");
        BookingConfirmedMessage? message;
        try
        {
            message = JsonSerializer.Deserialize<BookingConfirmedMessage>(payload, OperationResult.JsonOptions);
        }
        catch (JsonException)
        {
            throw Permanent("INVALID_MESSAGE_JSON");
        }
        if (message is null)
            throw Permanent("EMPTY_MESSAGE");
        Validate(message);
        if (message.MessageId != messageId || message.BookingId != aggregateId)
            throw Permanent("ENVELOPE_IDENTITY_MISMATCH");
        return message;
    }

    public static void Validate(BookingConfirmedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (message.SchemaVersion != 1)
            throw Permanent("UNSUPPORTED_SCHEMA_VERSION");
        if (message.MessageId == Guid.Empty || message.HoldId == Guid.Empty || message.Quantity <= 0 ||
            !IsIdentifier(message.BookingId) || !IsIdentifier(message.VoyageId) || message.ConfirmedAt == default)
            throw Permanent("INVALID_MESSAGE_FIELDS");
    }

    private static bool IsIdentifier(string? value) => !string.IsNullOrWhiteSpace(value) &&
        value.Length <= 128 && !value.Any(char.IsControl);
    private static DeliveryFailureException Permanent(string code) => new(DeliveryFailureKind.Permanent, code);
}
