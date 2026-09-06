using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Dapper;

namespace CapacityBooking.Infrastructure;

/// <summary>Applies ordered embedded SQL migrations atomically; rejects edits to applied files.</summary>
public sealed class MigrationRunner(Database database)
{
    public async Task MigrateAsync(CancellationToken ct = default)
    {
        await using var connection = await database.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition("SELECT pg_advisory_xact_lock(836492176234)",
            transaction: transaction, cancellationToken: ct));
        await connection.ExecuteAsync(new CommandDefinition("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version text PRIMARY KEY,
                sha256 text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """, transaction: transaction, cancellationToken: ct));

        var assembly = Assembly.GetExecutingAssembly();
        foreach (var name in assembly.GetManifestResourceNames()
                     .Where(n => n.Contains(".Migrations.", StringComparison.Ordinal) && n.EndsWith(".sql", StringComparison.Ordinal))
                     .Order(StringComparer.Ordinal))
        {
            await using var stream = assembly.GetManifestResourceStream(name)!;
            using var reader = new StreamReader(stream, Encoding.UTF8);
            var sql = await reader.ReadToEndAsync(ct);
            var checksum = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql)));
            var previous = await connection.QuerySingleOrDefaultAsync<string>(new CommandDefinition(
                "SELECT sha256 FROM schema_migrations WHERE version=@name", new { name }, transaction,
                cancellationToken: ct));
            if (previous is not null)
            {
                if (previous != checksum) throw new InvalidOperationException($"Applied migration was modified: {name}");
                continue;
            }
            await connection.ExecuteAsync(new CommandDefinition(sql, transaction: transaction, cancellationToken: ct));
            await connection.ExecuteAsync(new CommandDefinition(
                "INSERT INTO schema_migrations(version,sha256) VALUES(@name,@checksum)",
                new { name, checksum }, transaction, cancellationToken: ct));
        }
        await transaction.CommitAsync(ct);
    }
}
