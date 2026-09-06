using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using CapacityBooking.Application;
using CapacityBooking.Infrastructure;
using Dapper;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class HealthReadinessTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Fact]
    public async Task DisabledWorkersAreExplicitAndCompatibleSchemaIsReady()
    {
        using var response = await Db.Client.GetAsync("/health");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var health = (await response.Content.ReadFromJsonAsync<ServiceHealthSnapshot>())!;
        Assert.True(health.Ready);
        Assert.Equal("compatible", health.Database.Code);
        Assert.All(health.Workers, w => Assert.Equal("disabled", w.Status));
    }

    [Theory]
    [InlineData("checksum")]
    [InlineData("missing")]
    [InlineData("future")]
    public async Task ReadinessRejectsIncompatibleMigrationHistory(string fault)
    {
        var migration = MigrationRunner.GetMigrations()[^1];
        try
        {
            if (fault == "checksum")
                await Db.ExecuteAsync("UPDATE schema_migrations SET sha256='incorrect' WHERE version=@Version", migration);
            else if (fault == "missing")
                await Db.ExecuteAsync("DELETE FROM schema_migrations WHERE version=@Version", migration);
            else
                await Db.ExecuteAsync("INSERT INTO schema_migrations(version,sha256) VALUES('unknown-future-version','unknown')");

            using var ready = await Db.Client.GetAsync("/health/ready");
            Assert.Equal(HttpStatusCode.ServiceUnavailable, ready.StatusCode);
            var health = (await ready.Content.ReadFromJsonAsync<ServiceHealthSnapshot>())!;
            Assert.Equal("schema_mismatch", health.Database.Code);
            using var live = await Db.Client.GetAsync("/health/live");
            Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        }
        finally
        {
            await Db.ExecuteAsync("DELETE FROM schema_migrations WHERE version='unknown-future-version'");
            await Db.ExecuteAsync("INSERT INTO schema_migrations(version,sha256) VALUES(@Version,@Sha256) ON CONFLICT(version) DO UPDATE SET sha256=excluded.sha256", migration);
        }
    }

    [Theory]
    [InlineData("booking_confirmations", "quantity")]
    [InlineData("inbox", "processed_at")]
    public async Task CorrectHistoryDoesNotHideAMissingRequiredColumn(string table, string column)
    {
        // Identifiers are fixed test cases, not user input.
        await Db.ExecuteAsync($"ALTER TABLE {table} RENAME COLUMN {column} TO missing_column");
        try
        {
            using var response = await Db.Client.GetAsync("/health/ready");
            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            var health = (await response.Content.ReadFromJsonAsync<ServiceHealthSnapshot>())!;
            Assert.Equal("schema_unavailable", health.Database.Code);
        }
        finally { await Db.ExecuteAsync($"ALTER TABLE {table} RENAME COLUMN missing_column TO {column}"); }
    }

    [Fact]
    public async Task DatabaseProbeTimesOutWhenSchemaAccessIsBlocked()
    {
        await using var blocker = await Db.OpenAsync();
        await using var transaction = await blocker.BeginTransactionAsync();
        await blocker.ExecuteAsync("LOCK TABLE schema_migrations IN ACCESS EXCLUSIVE MODE", transaction: transaction);
        var probe = new DatabaseReadinessProbe(new Database(Db.ConnectionString),
            Options.Create(new HealthOptions { DatabaseTimeout = TimeSpan.FromMilliseconds(150) }));
        var watch = Stopwatch.StartNew();
        var health = await probe.CheckAsync();
        Assert.False(health.Ready);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(3), "Health must not wait for the normal database command timeout.");
        await transaction.RollbackAsync();
        Assert.True((await probe.CheckAsync()).Ready);
    }

    [Fact]
    public async Task BlockedWorkerMakesReadinessFailWhileLivenessRespondsThenRecovers()
    {
        var observer = new TestObserver();
        using var gate = observer.Gate("worker.expiry.before-poll");
        await using var host = new WorkerApplication(Db.ConnectionString, observer);
        using var client = host.CreateClient();
        await gate.WaitForEntryAsync();
        await DatabaseFixture.WaitUntilAsync(async () =>
        {
            using var ready = await client.GetAsync("/health/ready");
            var state = (await ready.Content.ReadFromJsonAsync<ServiceHealthSnapshot>())!;
            return ready.StatusCode == HttpStatusCode.ServiceUnavailable && state.Workers.Single(w => w.Name == "expiry").Status == "stalled";
        }, "The actual blocked worker was not reflected by readiness.");
        using var live = await client.GetAsync("/health/live");
        Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        gate.Release();
        await WaitForHealthyAsync(client);
    }

    [Fact]
    public async Task IdleWorkersRemainHealthyAndCaughtFailuresAreVisible()
    {
        var observer = new TestObserver();
        await using var host = new WorkerApplication(Db.ConnectionString, observer);
        using var client = host.CreateClient();
        await WaitForHealthyAsync(client);
        var healthy = (await client.GetFromJsonAsync<ServiceHealthSnapshot>("/health"))!;
        Assert.All(healthy.Workers, w => Assert.Equal(0, w.CompletedItems));
        observer.FailAt("worker.expiry.before-poll");
        await DatabaseFixture.WaitUntilAsync(async () =>
        {
            using var response = await client.GetAsync("/health");
            var state = (await response.Content.ReadFromJsonAsync<ServiceHealthSnapshot>())!;
            return !state.Ready && state.Workers.Single(w => w.Name == "expiry").ConsecutiveFailures >= 2;
        }, "Caught worker failures were hidden from readiness.");
        observer.Reset();
        await WaitForHealthyAsync(client);
    }

    [Fact]
    public async Task RepeatedInternalExpiryBudgetTimeoutsCannotMasqueradeAsSuccessfulPolls()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        await Db.ExecuteAsync("UPDATE capacity_holds SET created_at=clock_timestamp()-interval '3 minutes', expires_at=clock_timestamp()-interval '1 minute'");
        var observer = new TestObserver();
        using var gate = observer.Gate("business.after-locks", hold.HoldId.ToString("D"));
        await using var host = new WorkerApplication(Db.ConnectionString, observer);
        using var client = host.CreateClient();
        await gate.WaitForEntryAsync();
        try
        {
            await DatabaseFixture.WaitUntilAsync(async () =>
            {
                using var response = await client.GetAsync("/health/ready");
                var state = (await response.Content.ReadFromJsonAsync<ServiceHealthSnapshot>())!;
                return !state.Ready && state.Workers.Single(w => w.Name == "expiry").ConsecutiveFailures >= 2;
            }, "Unresolved expiry budget timeouts were reported as successful polling.");
            Assert.Equal("Active", await Db.ScalarAsync<string>("SELECT state FROM capacity_holds"));
            await Db.AssertAccountingAsync(1, 0);
        }
        finally { gate.Release(); }
        await WaitForHealthyAsync(client);
        await DatabaseFixture.WaitUntilAsync(async () => await Db.ScalarAsync<string>("SELECT state FROM capacity_holds") == "Expired",
            "The deferred hold did not recover after the blocking condition cleared.");
        await Db.AssertAccountingAsync(0, 0);
    }

    [Fact]
    public async Task QuarantineAndOldBacklogAreReportedAsDegradedWithoutRejectingDurableCommands()
    {
        await Db.SeedAsync(2);
        var poison = await Db.CreateHoldAsync("poison", key: "poison");
        using var confirmed = await Db.ConfirmAsync(poison.HoldId);
        await Db.ExecuteAsync("UPDATE outbox SET event_type='UnsupportedEvent'");
        await Db.PublishAsync();
        var pending = await Db.CreateHoldAsync("pending", key: "pending");
        using var confirmation = await Db.ConfirmAsync(pending.HoldId, "pending-confirm");
        await Db.ExecuteAsync("UPDATE outbox SET occurred_at=clock_timestamp()-interval '3 minutes' WHERE delivery_state='Pending'");
        using var response = await Db.Client.GetAsync("/health");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var health = (await response.Content.ReadFromJsonAsync<ServiceHealthSnapshot>())!;
        Assert.Equal("degraded", health.Status);
        Assert.Equal(1, health.Database.QuarantinedOutbox);
        Assert.Equal(1, health.Database.PendingOutbox);
        Assert.True(health.Database.OldestOutboxAgeSeconds >= 180);
    }

    private static Task WaitForHealthyAsync(HttpClient client) => DatabaseFixture.WaitUntilAsync(async () =>
    {
        using var response = await client.GetAsync("/health");
        return response.StatusCode == HttpStatusCode.OK;
    }, "Worker health did not recover after successful polling.");

    private sealed class WorkerApplication(string connectionString, TestObserver observer) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:Database"] = connectionString,
                ["Workers:Enabled"] = "true",
                ["Workers:PollIntervalMilliseconds"] = "25",
                ["Health:WorkerStaleAfter"] = "00:00:00.500"
                ,
                ["Expiry:PollTimeBudget"] = "00:00:00.150"
            }));
            builder.ConfigureServices(services =>
            {
                services.RemoveAll<Database>();
                services.AddSingleton(new Database(connectionString));
                services.RemoveAll<IExecutionObserver>();
                services.AddSingleton<IExecutionObserver>(observer);
            });
        }
    }
}

public sealed class WorkerHealthTests
{
    [Fact]
    public void MonotonicProgressKeepsAProductiveLongBatchHealthyButErrorsDoNot()
    {
        var clock = new ManualClock();
        var workers = new WorkerHealthRegistry(clock);
        workers.Started("expiry", true);
        workers.Started("outbox", true);
        workers.PollSucceeded("outbox");
        workers.PollStarted("expiry");
        clock.Advance(TimeSpan.FromSeconds(20));
        workers.ItemCompleted("expiry");
        clock.Advance(TimeSpan.FromSeconds(20));
        Assert.Equal("healthy", workers.Snapshot(true, TimeSpan.FromSeconds(30)).Single(w => w.Name == "expiry").Status);
        workers.PollFailed("expiry", new InvalidOperationException());
        clock.Advance(TimeSpan.FromSeconds(20));
        Assert.Equal("stalled", workers.Snapshot(true, TimeSpan.FromSeconds(30)).Single(w => w.Name == "expiry").Status);
        workers.PollSucceeded("expiry");
        Assert.Equal("healthy", workers.Snapshot(true, TimeSpan.FromSeconds(30)).Single(w => w.Name == "expiry").Status);
        workers.Stopped("expiry");
        Assert.Equal("stopped", workers.Snapshot(true, TimeSpan.FromSeconds(30)).Single(w => w.Name == "expiry").Status);
    }

    private sealed class ManualClock : TimeProvider
    {
        private long _timestamp;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => _timestamp;
        public void Advance(TimeSpan elapsed) => _timestamp += elapsed.Ticks;
    }
}
