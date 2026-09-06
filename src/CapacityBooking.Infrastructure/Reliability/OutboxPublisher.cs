using System.Data;
using CapacityBooking.Application;
using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CapacityBooking.Infrastructure.Reliability;

/// <summary>Recoverable short leases, classified delivery failures and an audited manual recovery state.</summary>
public sealed class OutboxPublisher : IOutboxPublisher
{
    private readonly Database _database;
    private readonly IMessageTransport _transport;
    private readonly DeliveryOptions _options;
    private readonly IExecutionObserver _observer;
    private readonly ILogger<OutboxPublisher> _logger;
    private readonly IWorkerProgress? _progress;

    public OutboxPublisher(Database database, IMessageTransport transport, IOptions<DeliveryOptions> options,
        IExecutionObserver observer, ILogger<OutboxPublisher> logger, IWorkerProgress? progress = null)
    {
        _database = database;
        _transport = transport;
        _options = options.Value;
        _observer = observer;
        _logger = logger;
        _progress = progress;
        if (_options.LeaseDuration <= TimeSpan.Zero || _options.RetryBaseDelay <= TimeSpan.Zero ||
            _options.MaxRetryDelay < _options.RetryBaseDelay || _options.MaxFailuresPerCycle is < 1 or > 10000)
            throw new ArgumentOutOfRangeException(nameof(options), "Delivery timings must be positive and failure budget must be between 1 and 10000.");
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

            ClassifiedDeliveryFailure? failure = null;
            BookingConfirmedMessage? message = null;
            try
            {
                message = BookingConfirmedValidation.Read(row.EventType, row.MessageId, row.AggregateId, row.Payload);
                await _transport.PublishAsync(message, ct);
                await _observer.ReachedAsync("outbox.after-publish", row.MessageId.ToString("D"), ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // A cancellation or dead process leaves a reclaimable lease, not a spent failure budget.
                throw;
            }
            catch (Exception exception)
            {
                failure = DeliveryFailureClassifier.Classify(exception);
            }

            // Database outcome/audit errors propagate to the worker. They do not classify a valid
            // message as poison: the already committed claim remains recoverable after lease expiry.
            var outcome = await CompleteAttemptAsync(row, leaseToken, failure, ct);
            _progress?.ItemCompleted("outbox");
            if (outcome == "Published")
            {
                published++;
                _logger.LogInformation(
                    "Outbox published: MessageId {MessageId}, BookingId {BookingId}, VoyageId {VoyageId}, HoldId {HoldId}, Attempt {Attempt}, TraceId {TraceId}",
                    row.MessageId, row.AggregateId, message?.VoyageId, message?.HoldId, row.Attempts, message?.TraceId);
            }
            else
            {
                _logger.LogWarning(
                    "Outbox delivery outcome {Outcome}: MessageId {MessageId}, BookingId {BookingId}, Attempt {Attempt}, FailureKind {FailureKind}, ReasonCode {ReasonCode}",
                    outcome, row.MessageId, row.AggregateId, row.Attempts, failure?.Kind, failure?.ReasonCode);
            }
        }
        return published;
    }

    private async Task<OutboxRow?> ClaimAsync(Guid leaseToken, CancellationToken ct)
    {
        await using var connection = await _database.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var row = await connection.QuerySingleOrDefaultAsync<OutboxRow>(new CommandDefinition("""
            WITH candidate AS (
                SELECT message_id FROM outbox
                WHERE delivery_state='Pending'
                  AND next_attempt_at <= statement_timestamp()
                  AND (lease_until IS NULL OR lease_until <= statement_timestamp())
                ORDER BY next_attempt_at, occurred_at, message_id
                FOR UPDATE SKIP LOCKED LIMIT 1
            )
            UPDATE outbox AS o
            SET lease_token=@LeaseToken, lease_until=clock_timestamp()+@LeaseDuration,
                attempts=o.attempts+1
            FROM candidate AS c WHERE o.message_id=c.message_id
            RETURNING o.message_id,o.event_type,o.aggregate_id,o.payload,o.attempts,o.failure_count,o.state_version
            """, new { LeaseToken = leaseToken, _options.LeaseDuration }, transaction, cancellationToken: ct));
        if (row is not null)
        {
            await connection.ExecuteAsync(new CommandDefinition("""
                INSERT INTO outbox_delivery_audit
                    (message_id,action,attempt,failure_count,state_version,lease_token,actor)
                VALUES (@MessageId,'Claimed',@Attempts,@FailureCount,@StateVersion,@LeaseToken,'outbox-publisher')
                """, new { row.MessageId, row.Attempts, row.FailureCount, row.StateVersion, LeaseToken = leaseToken },
                transaction, cancellationToken: ct));
            await _observer.ReachedAsync("outbox.before-claim-commit", row.MessageId.ToString("D"), ct);
        }
        await transaction.CommitAsync(ct);
        return row;
    }

    private async Task<string> CompleteAttemptAsync(OutboxRow row, Guid leaseToken,
        ClassifiedDeliveryFailure? failure, CancellationToken ct)
    {
        var failureCount = failure is null ? row.FailureCount : checked(row.FailureCount + 1);
        var quarantine = failure is not null &&
            (failure.Kind == DeliveryFailureKind.Permanent || failureCount >= _options.MaxFailuresPerCycle);
        var outcome = failure is null ? "Published" : quarantine ? "Quarantined" : "RetryScheduled";
        var state = failure is null ? "Published" : quarantine ? "Quarantined" : "Pending";
        var quarantineCode = quarantine
            ? failure!.Kind == DeliveryFailureKind.Permanent ? failure.ReasonCode : "FAILURE_BUDGET_EXHAUSTED"
            : null;
        var retryDelay = RetryDelay(failureCount);
        await using var connection = await _database.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var updated = await connection.QuerySingleOrDefaultAsync<OutcomeRow>(new CommandDefinition("""
            UPDATE outbox SET delivery_state=@State,failure_count=@FailureCount,state_version=state_version+1,
                published_at=CASE WHEN @State='Published' THEN clock_timestamp() ELSE NULL END,
                quarantined_at=CASE WHEN @State='Quarantined' THEN clock_timestamp() ELSE NULL END,
                quarantine_reason_code=@QuarantineCode,last_error=@ErrorCode,
                next_attempt_at=CASE WHEN @State='Pending' THEN clock_timestamp()+@RetryDelay ELSE next_attempt_at END,
                lease_token=NULL,lease_until=NULL
            WHERE message_id=@MessageId AND lease_token=@LeaseToken AND delivery_state='Pending'
            RETURNING failure_count,state_version
            """, new
        {
            State = state,
            FailureCount = failureCount,
            QuarantineCode = quarantineCode,
            ErrorCode = failure?.ReasonCode,
            RetryDelay = retryDelay,
            row.MessageId,
            LeaseToken = leaseToken
        }, transaction, cancellationToken: ct));
        if (updated is null)
            outcome = "StaleOutcomeIgnored";

        await connection.ExecuteAsync(new CommandDefinition("""
            INSERT INTO outbox_delivery_audit
                (message_id,action,attempt,failure_count,state_version,lease_token,actor,failure_kind,reason_code)
            VALUES (@MessageId,@Outcome,@Attempts,@FailureCount,@StateVersion,@LeaseToken,'outbox-publisher',@FailureKind,@ReasonCode)
            """, new
        {
            row.MessageId,
            Outcome = outcome,
            row.Attempts,
            FailureCount = updated?.FailureCount ?? row.FailureCount,
            StateVersion = updated?.StateVersion ?? row.StateVersion,
            LeaseToken = leaseToken,
            FailureKind = failure?.Kind.ToString(),
            ReasonCode = failure?.ReasonCode
        }, transaction, cancellationToken: ct));
        await _observer.ReachedAsync("outbox.before-outcome-commit", row.MessageId.ToString("D"), ct);
        await transaction.CommitAsync(ct);
        return outcome;
    }

    private TimeSpan RetryDelay(int failures)
    {
        var multiplier = Math.Pow(2, Math.Clamp(failures - 1, 0, 30));
        return TimeSpan.FromMilliseconds(Math.Min(_options.MaxRetryDelay.TotalMilliseconds,
            _options.RetryBaseDelay.TotalMilliseconds * multiplier));
    }

    private sealed class OutboxRow
    {
        public Guid MessageId { get; set; }
        public string EventType { get; set; } = "";
        public string AggregateId { get; set; } = "";
        public string Payload { get; set; } = "";
        public int Attempts { get; set; }
        public int FailureCount { get; set; }
        public long StateVersion { get; set; }
    }

    private sealed class OutcomeRow
    {
        public int FailureCount { get; set; }
        public long StateVersion { get; set; }
    }
}
