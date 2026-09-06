namespace CapacityBooking.Domain;

public enum HoldState { Active, Consumed, Expired, Cancelled }

/// <summary>A lifecycle entity owned by its voyage's capacity aggregate.</summary>
public sealed class CapacityHold
{
    public Guid HoldId { get; }
    public string BookingId { get; }
    public string VoyageId { get; }
    public CapacityUnits Quantity { get; }
    public HoldState State { get; private set; }
    public DateTimeOffset CreatedAt { get; }
    public HoldDeadline Deadline { get; }
    public DateTimeOffset? CompletedAt { get; private set; }

    public CapacityHold(Guid holdId, string bookingId, string voyageId, CapacityUnits quantity,
        HoldState state, DateTimeOffset createdAt, DateTimeOffset expiresAt, DateTimeOffset? completedAt)
    {
        if (holdId == Guid.Empty || string.IsNullOrWhiteSpace(bookingId) || string.IsNullOrWhiteSpace(voyageId))
            throw new ArgumentException("A hold requires durable hold, booking and voyage identities.");
        if (quantity.Value <= 0)
            throw new ArgumentOutOfRangeException(nameof(quantity));
        if (!Enum.IsDefined(state) || expiresAt <= createdAt ||
            (state == HoldState.Active) != (completedAt is null))
            throw new ArgumentException("The persisted hold lifecycle is invalid.");

        HoldId = holdId;
        BookingId = bookingId;
        VoyageId = voyageId;
        Quantity = quantity;
        State = state;
        CreatedAt = createdAt;
        Deadline = new HoldDeadline(expiresAt);
        CompletedAt = completedAt;
    }

    internal void Consume(DateTimeOffset decisionTime)
    {
        if (State != HoldState.Active || !Deadline.CanConfirmAt(decisionTime))
            throw new InvalidOperationException("Only an active, unexpired hold can be consumed.");
        Complete(HoldState.Consumed, decisionTime);
    }

    internal bool Expire(DateTimeOffset decisionTime)
    {
        if (State != HoldState.Active || !Deadline.IsExpiredAt(decisionTime))
            return false;
        Complete(HoldState.Expired, decisionTime);
        return true;
    }

    internal bool Cancel(DateTimeOffset decisionTime)
    {
        if (State != HoldState.Active)
            return false;
        // Crossing the deadline is always an expiry, including a late DELETE.
        Complete(Deadline.IsExpiredAt(decisionTime) ? HoldState.Expired : HoldState.Cancelled, decisionTime);
        return true;
    }

    private void Complete(HoldState state, DateTimeOffset at)
    {
        State = state;
        CompletedAt = at;
    }
}
