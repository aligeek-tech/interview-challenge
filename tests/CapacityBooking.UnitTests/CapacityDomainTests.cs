using CapacityBooking.Domain;
using Xunit;

namespace CapacityBooking.UnitTests;

public sealed class CapacityDomainTests
{
    private static readonly DateTimeOffset Now = new(2030, 1, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void ConfirmMovesTheReservationToConfirmedAndTerminalOperationsCannotReleaseIt()
    {
        var capacity = new VoyageCapacity("voyage", 3, 0, 0, true);
        var booking = NewBooking(3);
        var hold = capacity.CreateHold(Guid.NewGuid(), booking, Now, TimeSpan.FromMinutes(2));
        Assert.Equal(0, capacity.Available);
        capacity.ConsumeHold(hold, Now.AddSeconds(1));
        booking.Confirm(hold, Now.AddSeconds(1));
        Assert.True(booking.IsConfirmed);
        Assert.Equal(0, capacity.Reserved);
        Assert.Equal(3, capacity.Confirmed);
        Assert.False(capacity.CancelHold(hold, Now.AddSeconds(2)));
        Assert.False(capacity.ExpireHold(hold, Now.AddMinutes(3)));
        Assert.Equal(3, capacity.Confirmed);
        Assert.Equal(HoldState.Consumed, hold.State);
    }

    [Fact]
    public void CancellationAtTheDeadlineIsAnExpiryAndReleasesOnlyOnce()
    {
        var capacity = new VoyageCapacity("voyage", 2, 0, 0, true);
        var hold = capacity.CreateHold(Guid.NewGuid(), NewBooking(2), Now, TimeSpan.FromMinutes(2));
        Assert.True(capacity.CancelHold(hold, Now.AddMinutes(2)));
        Assert.Equal(HoldState.Expired, hold.State);
        Assert.False(capacity.CancelHold(hold, Now.AddMinutes(3)));
        Assert.False(capacity.ExpireHold(hold, Now.AddMinutes(3)));
        Assert.Equal(2, capacity.Available);
    }

    [Fact]
    public void FailedDeadlineDecisionDoesNotMutateHoldOrCapacity()
    {
        var capacity = new VoyageCapacity("voyage", 1, 0, 0, true);
        var hold = capacity.CreateHold(Guid.NewGuid(), NewBooking(1), Now, TimeSpan.FromMinutes(2));
        Assert.Throws<InvalidOperationException>(() => capacity.ConsumeHold(hold, Now.AddMinutes(2)));
        Assert.Equal(HoldState.Active, hold.State);
        Assert.Equal(1, capacity.Reserved);
        Assert.Equal(0, capacity.Confirmed);
    }

    [Fact]
    public void ACapacityRootCannotConsumeAnotherVoyagesHold()
    {
        var source = new VoyageCapacity("voyage", 1, 0, 0, true);
        var other = new VoyageCapacity("other", 1, 1, 0, true);
        var hold = source.CreateHold(Guid.NewGuid(), NewBooking(1), Now, TimeSpan.FromMinutes(2));
        Assert.Throws<InvalidOperationException>(() => other.ConsumeHold(hold, Now.AddSeconds(1)));
        Assert.Equal(HoldState.Active, hold.State);
        Assert.Equal(1, other.Reserved);
    }

    [Fact]
    public void CapacityValidationDoesNotOverflowWhenCountersAreLarge()
    {
        Assert.Throws<ArgumentException>(() => new VoyageCapacity("voyage", int.MaxValue, int.MaxValue, int.MaxValue, true));
        var capacity = new VoyageCapacity("voyage", int.MaxValue, int.MaxValue - 1, 1, true);
        Assert.Equal(0, capacity.Available);
        Assert.Throws<InvalidOperationException>(() => capacity.CreateHold(Guid.NewGuid(), NewBooking(1), Now, TimeSpan.FromMinutes(2)));
    }

    [Fact]
    public void BookingCannotConfirmUsingAnUnconsumedOrAnotherBookingsHold()
    {
        var capacity = new VoyageCapacity("voyage", 1, 0, 0, true);
        var owner = NewBooking(1);
        var hold = capacity.CreateHold(Guid.NewGuid(), owner, Now, TimeSpan.FromMinutes(2));
        Assert.Throws<InvalidOperationException>(() => owner.Confirm(hold, Now));
        capacity.ConsumeHold(hold, Now.AddSeconds(1));
        var another = new Booking("other-booking", "voyage", "customer", new CapacityUnits(1));
        Assert.Throws<InvalidOperationException>(() => another.Confirm(hold, Now.AddSeconds(1)));
        Assert.False(another.IsConfirmed);
    }

    private static Booking NewBooking(int units) => new("booking", "voyage", "customer", new CapacityUnits(units));
}
