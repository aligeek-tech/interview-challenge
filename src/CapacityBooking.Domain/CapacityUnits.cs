namespace CapacityBooking.Domain;

/// <summary>Voyage capacity is measured in positive whole units.</summary>
public readonly record struct CapacityUnits
{
    public int Value { get; }

    public CapacityUnits(int value)
    {
        if (value <= 0)
            throw new ArgumentOutOfRangeException(nameof(value), "Capacity must be a positive number of units.");
        Value = value;
    }
}
