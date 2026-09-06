using System.Security.Cryptography;
using System.Text;
using Dapper;

namespace CapacityBooking.Infrastructure;

/// <summary>Applies ordered embedded SQL migrations atomically; rejects edits to applied files.</summary>
public sealed class MigrationRunner(Database database)
{
    private static readonly Lazy<IReadOnlyList<MigrationDefinition>> Manifest = new(() =>
    {
        var assembly = typeof(MigrationRunner).Assembly;
        var migrations = assembly.GetManifestResourceNames()
            .Where(n => n.Contains(".Migrations.", StringComparison.Ordinal) && n.EndsWith(".sql", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal)
            .Select(name =>
            {
                using var stream = assembly.GetManifestResourceStream(name)!;
                using var reader = new StreamReader(stream, Encoding.UTF8);
                var sql = reader.ReadToEnd();
                return new MigrationDefinition(name, sql, Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql))));
            }).ToArray();
        return Array.AsReadOnly(migrations);
    });

    public static IReadOnlyList<MigrationDefinition> GetMigrations() => Manifest.Value;

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

        var migrations = GetMigrations();
        var appliedVersions = await connection.QueryAsync<string>(new CommandDefinition(
            "SELECT version FROM schema_migrations", transaction: transaction, cancellationToken: ct));
        if (appliedVersions.Except(migrations.Select(m => m.Version), StringComparer.Ordinal).Any())
            throw new InvalidOperationException("The database contains a migration this application does not support.");

        foreach (var migration in migrations)
        {
            var (name, sql, checksum) = migration;
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

public sealed record MigrationDefinition(string Version, string Sql, string Sha256);
