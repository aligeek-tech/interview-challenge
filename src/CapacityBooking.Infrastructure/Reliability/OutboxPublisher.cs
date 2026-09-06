using System.Text.Json;
using CapacityBooking.Application;
using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CapacityBooking.Infrastructure.Reliability;

/// <summary>Short database claims plus expiring leases; publication is deliberately at least once.</summary>
public sealed class OutboxPublisher : IOutboxPublisher
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly Database _database;
    private readonly IMessageTransport _transport;
    private readonly DeliveryOptions _options;
    private readonly IExecutionObserver _observer;
    private readonly ILogger<OutboxPublisher> _logger;

    public OutboxPublisher(
        Database database,
        IMessageTransport transport,
        IOptions<DeliveryOptions> options,
        IExecutionObserver observer,
        ILogger<OutboxPublisher> logger)
    {
        _database = database;
        _transport = transport;
        _options = options.Value;
        _observer = observer;
        _logger = logger;
        if (_options.LeaseDuration <= TimeSpan.Zero || _options.RetryBaseDelay <= TimeSpan.Zero ||
            _options.MaxRetryDelay < _options.RetryBaseDelay)
            throw new ArgumentOutOfRangeException(nameof(options), "Delivery lease and retry delays must be positive, with maximum retry delay at least the base delay.");
    }

    public async Task<int> PublishBatchAsync(int batchSize = 100, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        var published = 0;
        for (var index = 0; index < batchSize; index++)
        {
            ct.ThrowIfCancellationRequested();
            var leaseToken = Guid.NewGuid();
            var row = await ClaimAsync(leaseToken, ct);
            if (row is null)
                break;

            try
            {
                if (row.EventType != "BookingConfirmed")
                    throw new InvalidOperationException("Unsupported outbox event type.");
                var message = JsonSerializer.Deserialize<BookingConfirmedMessage>(row.Payload, JsonOptions)
                    ?? throw new InvalidOperationException("Outbox payload is empty.");
                if (message.MessageId != row.MessageId || message.BookingId != row.AggregateId)
                    throw new InvalidOperationException("Outbox envelope and payload identities differ.");

                await _transport.PublishAsync(message, ct);
                await _observer.ReachedAsync("outbox.after-publish", row.MessageId.ToString("D"), ct);
                var marked = await MarkPublishedAsync(row.MessageId, leaseToken, ct);
                if (marked)
                {
                    published++;
                    _logger.LogInformation(
                        "Outbox published: MessageId {MessageId}, BookingId {BookingId}, VoyageId {VoyageId}, HoldId {HoldId}, Attempt {Attempt}, TraceId {TraceId}",
                        row.MessageId, message.BookingId, message.VoyageId, message.HoldId, row.Attempts, message.TraceId);
                }
                else
                {
                    _logger.LogWarning(
                        "Outbox delivery completed after lease ownership changed: MessageId {MessageId}, BookingId {BookingId}, Attempt {Attempt}",
                        row.MessageId, row.AggregateId, row.Attempts);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Leave the persisted lease intact. A new worker can reclaim it after its deadline.
                throw;
            }
            catch (Exception exception)
            {
                var retryDelay = RetryDelay(row.Attempts);
                // Error type is sufficient for classification and avoids storing broker/DB secrets.
                var errorType = exception.GetType().Name;
                var released = await ReleaseForRetryAsync(row.MessageId, leaseToken, retryDelay, errorType, ct);
                _logger.LogWarning(
                    "Outbox publication requires retry: MessageId {MessageId}, BookingId {BookingId}, Attempt {Attempt}, ErrorType {ErrorType}, RetryDelay {RetryDelay}, LeaseReleased {LeaseReleased}",
                    row.MessageId, row.AggregateId, row.Attempts, errorType, retryDelay, released);
            }
        }

        return published;
    }

    private async Task<OutboxRow?> ClaimAsync(Guid leaseToken, CancellationToken ct)
    {
        await using var connection = await _database.OpenAsync(ct);
        // One statement commits the claim before transport I/O. Claim just one row per delivery so
        // rows later in a large batch do not lose most of their lease waiting behind slow publishes.
        return await connection.QuerySingleOrDefaultAsync<OutboxRow>(new CommandDefinition("""
            WITH candidate AS (
                SELECT message_id FROM outbox
                WHERE published_at IS NULL
                  AND next_attempt_at <= statement_timestamp()
                  AND (lease_until IS NULL OR lease_until <= statement_timestamp())
                ORDER BY next_attempt_at, occurred_at, message_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE outbox AS o
            SET lease_token = @LeaseToken,
                lease_until = clock_timestamp() + @LeaseDuration,
                attempts = o.attempts + 1
            FROM candidate AS c
            WHERE o.message_id = c.message_id
            RETURNING o.message_id, o.event_type, o.aggregate_id, o.payload, o.attempts
            """, new { LeaseToken = leaseToken, _options.LeaseDuration }, cancellationToken: ct));
    }

    private async Task<bool> MarkPublishedAsync(Guid messageId, Guid leaseToken, CancellationToken ct)
    {
        await using var connection = await _database.OpenAsync(ct);
        return await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE outbox
            SET published_at = clock_timestamp(), lease_token = NULL, lease_until = NULL, last_error = NULL
            WHERE message_id = @MessageId AND lease_token = @LeaseToken AND published_at IS NULL
            """, new { MessageId = messageId, LeaseToken = leaseToken }, cancellationToken: ct)) == 1;
    }

    private async Task<bool> ReleaseForRetryAsync(Guid messageId, Guid leaseToken, TimeSpan retryDelay, string errorType, CancellationToken ct)
    {
        await using var connection = await _database.OpenAsync(ct);
        return await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE outbox
            SET next_attempt_at = clock_timestamp() + @RetryDelay,
                lease_token = NULL, lease_until = NULL, last_error = @ErrorType
            WHERE message_id = @MessageId AND lease_token = @LeaseToken AND published_at IS NULL
            """, new { MessageId = messageId, LeaseToken = leaseToken, RetryDelay = retryDelay, ErrorType = errorType }, cancellationToken: ct)) == 1;
    }

    private TimeSpan RetryDelay(int attempts)
    {
        var multiplier = Math.Pow(2, Math.Clamp(attempts - 1, 0, 30));
        return TimeSpan.FromMilliseconds(Math.Min(
            _options.MaxRetryDelay.TotalMilliseconds,
            _options.RetryBaseDelay.TotalMilliseconds * multiplier));
    }

    private sealed class OutboxRow
    {
        public Guid MessageId { get; set; }
        public string EventType { get; set; } = "";
        public string AggregateId { get; set; } = "";
        public string Payload { get; set; } = "";
        public int Attempts { get; set; }
    }
}
