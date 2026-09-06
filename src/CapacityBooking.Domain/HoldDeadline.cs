namespace CapacityBooking.Domain;

/// <summary>The deadline is exclusive for confirmation and inclusive for expiry.</summary>
public readonly record struct HoldDeadline(DateTimeOffset ExpiresAt)
{
    public bool CanConfirmAt(DateTimeOffset decisionTime) => decisionTime < ExpiresAt;
    public bool IsExpiredAt(DateTimeOffset decisionTime) => decisionTime >= ExpiresAt;
}
