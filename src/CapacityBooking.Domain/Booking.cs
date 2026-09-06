namespace CapacityBooking.Domain;

/// <summary>Owns the immutable logical request identity and its single confirmation.</summary>
public sealed class Booking
{
    public string BookingId { get; }
    public string VoyageId { get; }
    public string CustomerId { get; }
    public CapacityUnits Quantity { get; }
    public Guid? ConfirmedHoldId { get; private set; }
    public DateTimeOffset? ConfirmedAt { get; private set; }
    public bool IsConfirmed => ConfirmedHoldId.HasValue;

    public Booking(string bookingId, string voyageId, string customerId, CapacityUnits quantity,
        Guid? confirmedHoldId = null, DateTimeOffset? confirmedAt = null)
    {
        if (string.IsNullOrWhiteSpace(bookingId) || string.IsNullOrWhiteSpace(voyageId) ||
            string.IsNullOrWhiteSpace(customerId) || quantity.Value <= 0)
            throw new ArgumentException("A booking requires a valid logical request identity.");
        if (confirmedHoldId.HasValue != confirmedAt.HasValue || confirmedHoldId == Guid.Empty)
            throw new ArgumentException("Booking confirmation fields must be present together.");
        BookingId = bookingId;
        VoyageId = voyageId;
        CustomerId = customerId;
        Quantity = quantity;
        ConfirmedHoldId = confirmedHoldId;
        ConfirmedAt = confirmedAt;
    }

    public bool MatchesRequest(string customerId, string voyageId, int quantity) =>
        CustomerId == customerId && VoyageId == voyageId && Quantity.Value == quantity;

    public void Confirm(CapacityHold hold, DateTimeOffset decisionTime)
    {
        if (IsConfirmed || hold.State != HoldState.Consumed || hold.BookingId != BookingId ||
            hold.VoyageId != VoyageId || hold.Quantity != Quantity || hold.CompletedAt != decisionTime)
            throw new InvalidOperationException("A booking can confirm once, using its own consumed hold.");
        ConfirmedHoldId = hold.HoldId;
        ConfirmedAt = decisionTime;
    }
}
