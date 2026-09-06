using CapacityBooking.Application;
using CapacityBooking.Domain;

namespace CapacityBooking.Infrastructure.Business;

/// <summary>The same domain transition and persistence path serves every expiry trigger.</summary>
internal static class HoldExpiryTransition
{
    public static async Task<bool> ApplyAsync(BookingPersistenceSession session, VoyageCapacity capacity,
        CapacityHold hold, DateTimeOffset decisionTime, RequestContext context, string reason, CancellationToken ct)
    {
        if (!capacity.ExpireHold(hold, decisionTime))
            return false;
        await session.SaveCapacityAsync(capacity, ct);
        await session.SaveTransitionAsync(hold, ct);
        await session.AppendAuditAsync(hold, "CapacityHoldExpired", decisionTime, context, reason, ct);
        return true;
    }
}
