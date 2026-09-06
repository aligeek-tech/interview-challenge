using CapacityBooking.Application;

namespace CapacityBooking.Infrastructure.Reliability;

/// <summary>
/// Executable broker substitute: acknowledgement means the durable consumer transaction completed.
/// There is no volatile queue. A production adapter must wait for broker publisher confirmation.
/// </summary>
public sealed class LocalMessageTransport(IBookingConfirmedConsumer consumer) : IMessageTransport
{
    public async Task PublishAsync(BookingConfirmedMessage message, CancellationToken ct = default)
    {
        await consumer.ConsumeAsync(message, ct);
    }
}
