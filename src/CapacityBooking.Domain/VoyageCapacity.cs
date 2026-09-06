namespace CapacityBooking.Domain;

/// <summary>
/// Owns capacity allocation and hold lifecycle transitions. Persistence locks the
/// root and loads only the target hold, rather than an unbounded history collection.
/// </summary>
public sealed class VoyageCapacity
{
    public string VoyageId { get; }
    public int Total { get; }
    public int Reserved { get; private set; }
    public int Confirmed { get; private set; }
    public bool IsOpen { get; }
    public int Available => Total - Reserved - Confirmed;

    public VoyageCapacity(string voyageId, int total, int reserved, int confirmed, bool isOpen)
    {
        if (string.IsNullOrWhiteSpace(voyageId) || total < 0 || reserved < 0 || confirmed < 0 ||
            (long)reserved + confirmed > total)
            throw new ArgumentException("Voyage capacity accounting is invalid.");
        VoyageId = voyageId;
        Total = total;
        Reserved = reserved;
        Confirmed = confirmed;
        IsOpen = isOpen;
    }

    public CapacityHold CreateHold(Guid holdId, Booking booking, DateTimeOffset decisionTime, TimeSpan ttl)
    {
        if (!IsOpen || booking.IsConfirmed || booking.VoyageId != VoyageId ||
            booking.Quantity.Value <= 0 || booking.Quantity.Value > Available || ttl <= TimeSpan.Zero)
            throw new InvalidOperationException("The voyage cannot accept this capacity hold.");

        var hold = new CapacityHold(holdId, booking.BookingId, VoyageId, booking.Quantity,
            HoldState.Active, decisionTime, decisionTime.Add(ttl), null);
        Reserved += booking.Quantity.Value;
        return hold;
    }

    public void ConsumeHold(CapacityHold hold, DateTimeOffset decisionTime)
    {
        EnsureOwned(hold);
        EnsureReserved(hold);
        hold.Consume(decisionTime);
        Reserved -= hold.Quantity.Value;
        Confirmed += hold.Quantity.Value;
    }

    public bool ExpireHold(CapacityHold hold, DateTimeOffset decisionTime)
    {
        EnsureOwned(hold);
        if (hold.State != HoldState.Active || !hold.Deadline.IsExpiredAt(decisionTime))
            return false;
        EnsureReserved(hold);
        if (!hold.Expire(decisionTime))
            return false;
        Reserved -= hold.Quantity.Value;
        return true;
    }

    public bool CancelHold(CapacityHold hold, DateTimeOffset decisionTime)
    {
        EnsureOwned(hold);
        if (hold.State != HoldState.Active)
            return false;
        EnsureReserved(hold);
        if (!hold.Cancel(decisionTime))
            return false;
        Reserved -= hold.Quantity.Value;
        return true;
    }

    private void EnsureOwned(CapacityHold hold)
    {
        if (hold.VoyageId != VoyageId)
            throw new InvalidOperationException("A capacity root cannot mutate another voyage's hold.");
    }

    private void EnsureReserved(CapacityHold hold)
    {
        if (Reserved < hold.Quantity.Value)
            throw new InvalidOperationException("Hold quantity is inconsistent with reserved capacity.");
    }
}
