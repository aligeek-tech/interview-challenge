using CapacityBooking.Application;
using CapacityBooking.Domain;

namespace CapacityBooking.Infrastructure.Business;

internal static class BookingResponses
{
    public static HoldView Hold(CapacityHold hold) => new(hold.HoldId, hold.BookingId, hold.VoyageId,
        hold.Quantity.Value, hold.State.ToString(), hold.CreatedAt, hold.Deadline.ExpiresAt, hold.CompletedAt);

    public static ConfirmationView Confirmation(Booking booking) => new(booking.BookingId, booking.VoyageId,
        booking.ConfirmedHoldId ?? throw new InvalidOperationException("Booking is not confirmed."),
        booking.Quantity.Value, booking.ConfirmedAt ?? throw new InvalidOperationException("Booking is not confirmed."));

    public static OperationResult Error(int statusCode, string code, string message) =>
        OperationResult.From(statusCode, new ApiError(code, message));
}
