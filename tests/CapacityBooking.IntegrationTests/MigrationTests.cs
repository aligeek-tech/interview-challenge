using System.Text.Json;
using CapacityBooking.Application;
using CapacityBooking.Infrastructure;
using Dapper;
using Npgsql;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class MigrationTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    // Hash of the already-submitted migration. New functionality must not rewrite its history.
    private const string OriginalInitialSha256 = "9FA5388BA522E85D1A2068D0C3B156BA837A288997419D6C3427C5046AA4CAF1";

    [Fact]
    public async Task FreshDatabaseAppliesTheCompleteManifestAndRepeatingMigrationChangesNothing()
    {
        await using var isolated = await MigrationDatabase.CreateAsync(Db.ConnectionString);
        var manifest = MigrationRunner.GetMigrations();
        Assert.Equal(OriginalInitialSha256, InitialMigration().Sha256);
        var runner = new MigrationRunner(isolated.Database);
        await runner.MigrateAsync();
        var history = await isolated.ScalarAsync<string>("SELECT jsonb_agg(to_jsonb(m) ORDER BY version)::text FROM schema_migrations m");
        await runner.MigrateAsync();
        Assert.Equal(history, await isolated.ScalarAsync<string>("SELECT jsonb_agg(to_jsonb(m) ORDER BY version)::text FROM schema_migrations m"));
        Assert.Equal(manifest.Count, await isolated.ScalarAsync<int>("SELECT count(*)::int FROM schema_migrations"));
        foreach (var migration in manifest)
            Assert.Equal(migration.Sha256, await isolated.ScalarAsync<string>("SELECT sha256 FROM schema_migrations WHERE version=@Version", new { migration.Version }));
        Assert.True(await isolated.ScalarAsync<bool>("SELECT to_regclass('voyage_capacity') IS NOT NULL AND to_regclass('capacity_holds') IS NOT NULL AND to_regclass('bookings') IS NOT NULL AND to_regclass('outbox') IS NOT NULL AND to_regclass('inbox') IS NOT NULL"));
        Assert.True(await isolated.ScalarAsync<bool>("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='outbox' AND column_name='delivery_state')"));
    }

    [Fact]
    public async Task PopulatedInitialDatabaseUpgradesWithoutChangingExistingBusinessDataOrInitialHistory()
    {
        await using var isolated = await MigrationDatabase.CreateAsync(Db.ConnectionString);
        await InstallOriginalSchemaAsync(isolated);
        await SeedOriginalDataAsync(isolated);
        var initialHistory = await isolated.ScalarAsync<string>("SELECT row_to_json(m)::text FROM schema_migrations m");
        var before = await OriginalDataSnapshotAsync(isolated);

        var runner = new MigrationRunner(isolated.Database);
        await runner.MigrateAsync();
        Assert.Equal(before, await OriginalDataSnapshotAsync(isolated));
        Assert.Equal(initialHistory, await isolated.ScalarAsync<string>(
            "SELECT row_to_json(m)::text FROM schema_migrations m WHERE version=@Version", new { InitialMigration().Version }));
        Assert.Equal(OriginalInitialSha256, await isolated.ScalarAsync<string>(
            "SELECT sha256 FROM schema_migrations WHERE version=@Version", new { InitialMigration().Version }));
        Assert.Equal("Pending", await isolated.ScalarAsync<string>("SELECT delivery_state FROM outbox WHERE aggregate_id='legacy-pending'"));
        Assert.Equal("Published", await isolated.ScalarAsync<string>("SELECT delivery_state FROM outbox WHERE aggregate_id='legacy-published'"));
        Assert.Equal(0, await isolated.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE quarantined_at IS NOT NULL"));
        Assert.Equal(MigrationRunner.GetMigrations().Count, await isolated.ScalarAsync<int>("SELECT count(*)::int FROM schema_migrations"));

        var upgradedHistory = await isolated.ScalarAsync<string>("SELECT jsonb_agg(to_jsonb(m) ORDER BY version)::text FROM schema_migrations m");
        await runner.MigrateAsync();
        Assert.Equal(upgradedHistory, await isolated.ScalarAsync<string>("SELECT jsonb_agg(to_jsonb(m) ORDER BY version)::text FROM schema_migrations m"));
        Assert.Equal(before, await OriginalDataSnapshotAsync(isolated));
    }

    [Fact]
    public async Task ChangedAppliedChecksumRejectsUpgradeWithoutApplyingLaterMigrationsOrChangingData()
    {
        await using var isolated = await MigrationDatabase.CreateAsync(Db.ConnectionString);
        await InstallOriginalSchemaAsync(isolated);
        await SeedOriginalDataAsync(isolated);
        await isolated.ExecuteAsync("UPDATE schema_migrations SET sha256=@wrong WHERE version=@Version",
            new { wrong = new string('0', 64), InitialMigration().Version });
        var before = await OriginalDataSnapshotAsync(isolated);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => new MigrationRunner(isolated.Database).MigrateAsync());
        Assert.Contains(InitialMigration().Version, error.Message, StringComparison.Ordinal);
        Assert.Equal(1, await isolated.ScalarAsync<int>("SELECT count(*)::int FROM schema_migrations"));
        Assert.Equal(new string('0', 64), await isolated.ScalarAsync<string>("SELECT sha256 FROM schema_migrations"));
        Assert.False(await isolated.ScalarAsync<bool>("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='outbox' AND column_name='delivery_state')"));
        Assert.Equal(before, await OriginalDataSnapshotAsync(isolated));
    }

    private static MigrationDefinition InitialMigration()
    {
        var migration = Assert.Single(MigrationRunner.GetMigrations(), candidate => candidate.Version.EndsWith(".001_initial.sql", StringComparison.Ordinal));
        Assert.Equal(OriginalInitialSha256, migration.Sha256);
        return migration;
    }

    private static async Task InstallOriginalSchemaAsync(MigrationDatabase isolated)
    {
        var original = InitialMigration();
        await using var connection = await isolated.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await connection.ExecuteAsync("""
            CREATE TABLE schema_migrations (
                version text PRIMARY KEY,
                sha256 text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """, transaction: transaction);
        await connection.ExecuteAsync(original.Sql, transaction: transaction);
        await connection.ExecuteAsync("INSERT INTO schema_migrations(version,sha256) VALUES(@Version,@Sha256)",
            original, transaction);
        await transaction.CommitAsync();
    }

    private static async Task SeedOriginalDataAsync(MigrationDatabase isolated)
    {
        var now = DateTimeOffset.UtcNow;
        var activeHold = Guid.NewGuid();
        var pendingHold = Guid.NewGuid();
        var publishedHold = Guid.NewGuid();
        var pendingMessage = new BookingConfirmedMessage(Guid.NewGuid(), "legacy-pending", "legacy-voyage", pendingHold, 3, now, "legacy-trace");
        var publishedMessage = new BookingConfirmedMessage(Guid.NewGuid(), "legacy-published", "legacy-voyage", publishedHold, 1, now, "legacy-trace");
        await isolated.ExecuteAsync("""
            INSERT INTO voyage_capacity(voyage_id,total,reserved,confirmed,is_open)
            VALUES ('legacy-voyage',10,2,4,true);
            INSERT INTO bookings(booking_id,voyage_id,customer_id,quantity)
            VALUES ('legacy-active','legacy-voyage','legacy-customer',2),
                   ('legacy-pending','legacy-voyage','legacy-customer',3),
                   ('legacy-published','legacy-voyage','legacy-customer',1);
            INSERT INTO capacity_holds(hold_id,booking_id,voyage_id,quantity,state,created_at,expires_at,completed_at)
            VALUES (@activeHold,'legacy-active','legacy-voyage',2,'Active',@created,@expires,NULL),
                   (@pendingHold,'legacy-pending','legacy-voyage',3,'Consumed',@created,@expires,@now),
                   (@publishedHold,'legacy-published','legacy-voyage',1,'Consumed',@created,@expires,@now);
            UPDATE bookings SET confirmed_hold_id=@pendingHold,confirmed_at=@now WHERE booking_id='legacy-pending';
            UPDATE bookings SET confirmed_hold_id=@publishedHold,confirmed_at=@now WHERE booking_id='legacy-published';
            INSERT INTO idempotency_records(customer_id,operation,idempotency_key,fingerprint,status_code,response_json)
            VALUES ('legacy-customer','confirm-legacy','legacy-key','legacy-fingerprint',200,'{"unchanged":"original response"}');
            INSERT INTO outbox(message_id,event_type,aggregate_id,payload,occurred_at,published_at,attempts,next_attempt_at,last_error)
            VALUES (@pendingId,'BookingConfirmed','legacy-pending',@pendingPayload,@now,NULL,2,@now,'LegacyTransientError'),
                   (@publishedId,'BookingConfirmed','legacy-published',@publishedPayload,@now,@now,1,@now,NULL);
            INSERT INTO audit_transitions(booking_id,voyage_id,hold_id,transition,occurred_at,trace_id,details)
            VALUES ('legacy-pending','legacy-voyage',@pendingHold,'BookingConfirmed',@now,'legacy-trace','Original audit fact');
            """, new
        {
            activeHold,
            pendingHold,
            publishedHold,
            now,
            created = now.AddMinutes(-1),
            expires = now.AddMinutes(2),
            pendingId = pendingMessage.MessageId,
            publishedId = publishedMessage.MessageId,
            pendingPayload = JsonSerializer.Serialize(pendingMessage, DatabaseFixture.JsonOptions),
            publishedPayload = JsonSerializer.Serialize(publishedMessage, DatabaseFixture.JsonOptions)
        });
    }

    private static Task<string> OriginalDataSnapshotAsync(MigrationDatabase isolated) => isolated.ScalarAsync<string>("""
        SELECT jsonb_build_object(
          'capacity',(SELECT jsonb_agg(to_jsonb(v) ORDER BY voyage_id) FROM voyage_capacity v),
          'bookings',(SELECT jsonb_agg(to_jsonb(b) ORDER BY booking_id) FROM bookings b),
          'holds',(SELECT jsonb_agg(to_jsonb(h) ORDER BY hold_id) FROM capacity_holds h),
          'idempotency',(SELECT jsonb_agg(to_jsonb(i) ORDER BY customer_id,operation,idempotency_key) FROM idempotency_records i),
          'audit',(SELECT jsonb_agg(to_jsonb(a) ORDER BY audit_id) FROM audit_transitions a),
          'outbox',(SELECT jsonb_agg(to_jsonb(o) ORDER BY message_id) FROM
            (SELECT message_id,event_type,aggregate_id,payload,occurred_at,published_at,attempts,next_attempt_at,lease_token,lease_until,last_error FROM outbox) o)
        )::text
        """);

    private sealed class MigrationDatabase(string adminConnectionString, string databaseName, string connectionString) : IAsyncDisposable
    {
        public Database Database { get; } = new(connectionString);

        public static async Task<MigrationDatabase> CreateAsync(string existingConnectionString)
        {
            var name = "capacity_migration_test_" + Guid.NewGuid().ToString("N");
            await using var admin = new NpgsqlConnection(existingConnectionString);
            await admin.OpenAsync();
            await admin.ExecuteAsync($"CREATE DATABASE \"{name}\"");
            var builder = new NpgsqlConnectionStringBuilder(existingConnectionString) { Database = name, Pooling = false };
            return new MigrationDatabase(existingConnectionString, name, builder.ConnectionString);
        }

        public Task<NpgsqlConnection> OpenAsync() => Database.OpenAsync();

        public async Task ExecuteAsync(string sql, object? parameters = null)
        {
            await using var connection = await OpenAsync();
            await connection.ExecuteAsync(sql, parameters);
        }

        public async Task<T> ScalarAsync<T>(string sql, object? parameters = null)
        {
            await using var connection = await OpenAsync();
            return (await connection.ExecuteScalarAsync<T>(sql, parameters))!;
        }

        public async ValueTask DisposeAsync()
        {
            await using var admin = new NpgsqlConnection(adminConnectionString);
            await admin.OpenAsync();
            // Only the randomly named database created by this helper can be dropped here.
            await admin.ExecuteAsync($"DROP DATABASE IF EXISTS \"{databaseName}\" WITH (FORCE)");
        }
    }
}
