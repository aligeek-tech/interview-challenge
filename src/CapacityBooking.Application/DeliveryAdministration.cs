namespace CapacityBooking.Application;

public sealed record OutboxQuarantineItem(Guid MessageId, string EventType, string AggregateId,
    int Attempts, int FailureCount, long StateVersion, DateTimeOffset QuarantinedAt, string ReasonCode);

public sealed record OutboxRedriveRequest(Guid MessageId, long ExpectedVersion, Guid ActionId,
    string Actor, string Reason);

public enum OutboxRedriveStatus
{
    Redriven,
    NotFound,
    NotQuarantined,
    VersionConflict,
    ActionConflict,
    InvalidRequest
}

public sealed record OutboxRedriveResult(OutboxRedriveStatus Status, Guid MessageId,
    long? StateVersion, bool Replayed = false);

public interface IOutboxAdministration
{
    Task<IReadOnlyList<OutboxQuarantineItem>> ListQuarantinedAsync(int limit = 100,
        CancellationToken ct = default);
    Task<OutboxRedriveResult> RedriveAsync(OutboxRedriveRequest request,
        CancellationToken ct = default);
}
