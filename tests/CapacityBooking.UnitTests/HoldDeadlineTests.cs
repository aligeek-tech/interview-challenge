using CapacityBooking.Domain;
using Xunit;

namespace CapacityBooking.UnitTests;

public sealed class HoldDeadlineTests
{
    private static readonly DateTimeOffset Expiry = new(2030, 1, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void LastInstantBeforeDeadlineMayConfirm()
    {
        var deadline = new HoldDeadline(Expiry);
        Assert.True(deadline.CanConfirmAt(Expiry.AddTicks(-1)));
        Assert.False(deadline.IsExpiredAt(Expiry.AddTicks(-1)));
    }

    [Fact]
    public void ExactEqualityBelongsToExpiry()
    {
        var deadline = new HoldDeadline(Expiry);
        Assert.False(deadline.CanConfirmAt(Expiry));
        Assert.True(deadline.IsExpiredAt(Expiry));
    }

    [Fact]
    public void InstantAfterDeadlineCannotConfirm()
    {
        var deadline = new HoldDeadline(Expiry);
        Assert.False(deadline.CanConfirmAt(Expiry.AddTicks(1)));
        Assert.True(deadline.IsExpiredAt(Expiry.AddTicks(1)));
    }

    [Fact]
    public void EquivalentInstantsWithDifferentOffsetsHaveTheSameDeadlineDecision()
    {
        var deadline = new HoldDeadline(Expiry);
        var sameInstant = Expiry.ToOffset(TimeSpan.FromHours(3.5));
        Assert.False(deadline.CanConfirmAt(sameInstant));
        Assert.True(deadline.IsExpiredAt(sameInstant));
    }
}
