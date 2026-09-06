using System.Text.Json;

namespace CapacityBooking.Application;

public sealed record CreateHoldRequest(string BookingId, int Quantity);
public sealed record HoldView(Guid HoldId, string BookingId, string VoyageId, int Quantity,
    string State, DateTimeOffset CreatedAt, DateTimeOffset ExpiresAt, DateTimeOffset? CompletedAt);
public sealed record ConfirmationView(string BookingId, string VoyageId, Guid HoldId,
    int Quantity, DateTimeOffset ConfirmedAt);
public sealed record ApiError(string Code, string Message);
public sealed record RequestContext(string CustomerId, string TraceId);

public sealed record OperationResult(int StatusCode, string Json, bool Replayed = false)
{
    public static JsonSerializerOptions JsonOptions { get; } = new(JsonSerializerDefaults.Web);
    public static OperationResult From(int statusCode, object body, bool replayed = false) =>
        new(statusCode, JsonSerializer.Serialize(body, JsonOptions), replayed);
}

public interface IBookingService
{
    Task<OperationResult> CreateHoldAsync(string voyageId, CreateHoldRequest request,
        string idempotencyKey, RequestContext context, CancellationToken ct = default);
    Task<OperationResult> ConfirmAsync(Guid holdId, string idempotencyKey,
        RequestContext context, CancellationToken ct = default);
    Task<OperationResult> CancelAsync(Guid holdId, RequestContext context, CancellationToken ct = default);
    Task<OperationResult> GetAsync(Guid holdId, RequestContext context, CancellationToken ct = default);
}

public interface IExpiryService
{
    Task<int> ExpireDueAsync(int batchSize = 100, CancellationToken ct = default);
    Task<bool> ExpireAsync(Guid holdId, CancellationToken ct = default);
}

public sealed record BookingConfirmedMessage(Guid MessageId, string BookingId, string VoyageId,
    Guid HoldId, int Quantity, DateTimeOffset ConfirmedAt, string TraceId, int SchemaVersion = 1);

public interface IMessageTransport
{
    Task PublishAsync(BookingConfirmedMessage message, CancellationToken ct = default);
}

public interface IBookingConfirmedConsumer
{
    Task<bool> ConsumeAsync(BookingConfirmedMessage message, CancellationToken ct = default);
}

public interface IOutboxPublisher
{
    Task<int> PublishBatchAsync(int batchSize = 100, CancellationToken ct = default);
}

/// <summary>Internal orchestration seam used by tests; never exposed as an HTTP endpoint.</summary>
public interface IExecutionObserver
{
    Task ReachedAsync(string point, string resourceId, CancellationToken ct = default);
}

public sealed class NullExecutionObserver : IExecutionObserver
{
    public Task ReachedAsync(string point, string resourceId, CancellationToken ct = default) => Task.CompletedTask;
}

public sealed class BookingOptions
{
    public TimeSpan HoldTtl { get; set; } = TimeSpan.FromMinutes(2);
}

public sealed class DeliveryOptions
{
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(1);
}
