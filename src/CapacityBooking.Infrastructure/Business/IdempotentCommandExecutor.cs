using System.Data;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CapacityBooking.Application;
using Dapper;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace CapacityBooking.Infrastructure.Business;

/// <summary>Owns one claim/business/response transaction; collaborators cannot commit separately.</summary>
internal sealed class IdempotentCommandExecutor(Database database, ILogger logger)
{
    public async Task<OperationResult> ExecuteAsync(string operation, string key, string fingerprint,
        RequestContext context, Func<BookingPersistenceSession, Task<OperationResult>> execute, CancellationToken ct)
    {
        using var scope = logger.BeginScope(new Dictionary<string, object>
        {
            ["TraceId"] = context.TraceId ?? string.Empty,
            ["Operation"] = operation,
            ["IdempotencyKeyHash"] = Fingerprint(key)[..16]
        });
        await using var connection = await database.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var identity = new { context.CustomerId, Operation = operation, IdempotencyKey = key, Fingerprint = fingerprint };
        try
        {
            var claimed = await connection.ExecuteAsync(new CommandDefinition("""
                INSERT INTO idempotency_records (customer_id, operation, idempotency_key, fingerprint)
                VALUES (@CustomerId, @Operation, @IdempotencyKey, @Fingerprint)
                ON CONFLICT (customer_id, operation, idempotency_key) DO NOTHING
                """, identity, transaction, cancellationToken: ct));
            if (claimed == 0)
            {
                // A fresh Read Committed snapshot follows the concurrent uniqueness wait.
                var existing = await connection.QuerySingleAsync<IdempotencyRow>(new CommandDefinition("""
                    SELECT fingerprint, status_code, response_json FROM idempotency_records
                    WHERE customer_id = @CustomerId AND operation = @Operation AND idempotency_key = @IdempotencyKey
                    """, identity, transaction, cancellationToken: ct));
                if (existing.Fingerprint != fingerprint)
                {
                    await transaction.CommitAsync(ct);
                    logger.LogWarning("IdempotencyConflict: the same scoped key was used with a different request fingerprint.");
                    return BookingResponses.Error(409, "IDEMPOTENCY_CONFLICT", "This idempotency key was already used for a different request.");
                }
                if (existing.StatusCode is null || existing.ResponseJson is null)
                    throw new InvalidOperationException("An incomplete idempotency record became visible.");
                await transaction.CommitAsync(ct);
                logger.LogInformation("DuplicateCommandDetected: replaying persisted HTTP status {StatusCode}", existing.StatusCode);
                return new OperationResult(existing.StatusCode.Value, existing.ResponseJson, true);
            }

            var result = await execute(new BookingPersistenceSession(connection, transaction));
            var completed = await connection.ExecuteAsync(new CommandDefinition("""
                UPDATE idempotency_records SET status_code = @StatusCode, response_json = @Json
                WHERE customer_id = @CustomerId AND operation = @Operation AND idempotency_key = @IdempotencyKey
                """, new { context.CustomerId, Operation = operation, IdempotencyKey = key, result.StatusCode, result.Json },
                transaction, cancellationToken: ct));
            if (completed != 1)
                throw new InvalidOperationException("Idempotency completion must affect exactly one claimed row.");
            await transaction.CommitAsync(ct);
            logger.LogInformation("Business operation committed with HTTP status {StatusCode}", result.StatusCode);
            return result;
        }
        catch (PostgresException exception) when (exception.SqlState is "40P01" or "40001" or "55P03")
        {
            logger.LogWarning("Database concurrency conflict {SqlState}; the whole operation remains retryable", exception.SqlState);
            throw;
        }
    }

    public static string Fingerprint<T>(T value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value, OperationResult.JsonOptions))));

    private sealed class IdempotencyRow
    {
        public string Fingerprint { get; set; } = "";
        public int? StatusCode { get; set; }
        public string? ResponseJson { get; set; }
    }
}
