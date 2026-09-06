using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using CapacityBooking.Application;
using CapacityBooking.Infrastructure;
using Dapper;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace CapacityBooking.IntegrationTests;

[CollectionDefinition(Name)]
public sealed class DatabaseCollection : ICollectionFixture<DatabaseFixture>
{
    public const string Name = "isolated-postgresql";
}

public sealed class DatabaseFixture : IAsyncLifetime
{
    private string _adminConnectionString = "";
    private string _databaseName = "";
    private TestApplication? _primary;
    private TestApplication? _secondary;
    public string ConnectionString { get; private set; } = "";
    public TestObserver Observer { get; } = new();
    public HttpClient Client { get; private set; } = null!;
    public HttpClient SecondClient { get; private set; } = null!;
    public IServiceProvider Services => _primary!.Services;
    public static JsonSerializerOptions JsonOptions { get; } = new(JsonSerializerDefaults.Web);

    public async Task InitializeAsync()
    {
        var configured = Environment.GetEnvironmentVariable("TEST_DATABASE_URL");
        if (string.IsNullOrWhiteSpace(configured))
            throw new InvalidOperationException("Set TEST_DATABASE_URL to a PostgreSQL connection string whose role has CREATEDB. Tests create and drop only a randomly named capacity_booking_test_* database.");

        _adminConnectionString = configured;
        _databaseName = "capacity_booking_test_" + Guid.NewGuid().ToString("N");
        await using (var admin = new NpgsqlConnection(_adminConnectionString))
        {
            await admin.OpenAsync();
            await admin.ExecuteAsync($"CREATE DATABASE \"{_databaseName}\"");
        }

        var builder = new NpgsqlConnectionStringBuilder(configured)
        {
            Database = _databaseName,
            ApplicationName = "capacity-booking-integration",
            MaxPoolSize = 100,
            Timeout = 10,
            CommandTimeout = 20
        };
        ConnectionString = builder.ConnectionString;
        await new MigrationRunner(new Database(ConnectionString)).MigrateAsync();
        _primary = new TestApplication(ConnectionString, Observer);
        _secondary = new TestApplication(ConnectionString, Observer);
        Client = _primary.CreateClient();
        SecondClient = _secondary.CreateClient();
        Client.Timeout = TimeSpan.FromSeconds(40);
        SecondClient.Timeout = TimeSpan.FromSeconds(40);
    }

    public async Task ResetAsync()
    {
        Observer.Reset();
        await ExecuteAsync("TRUNCATE TABLE audit_transitions, booking_confirmations, inbox, outbox, idempotency_records, capacity_holds, bookings, voyage_capacity RESTART IDENTITY CASCADE");
    }

    public async Task DisposeAsync()
    {
        Client?.Dispose();
        SecondClient?.Dispose();
        if (_primary is not null) await _primary.DisposeAsync();
        if (_secondary is not null) await _secondary.DisposeAsync();
        NpgsqlConnection.ClearAllPools();
        if (_databaseName.Length == 0) return;
        await using var admin = new NpgsqlConnection(_adminConnectionString);
        await admin.OpenAsync();
        // The identifier is generated above from a fixed prefix plus GUID, never supplied by callers.
        await admin.ExecuteAsync($"DROP DATABASE IF EXISTS \"{_databaseName}\" WITH (FORCE)");
    }

    public async Task<NpgsqlConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

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

    public async Task<T> QuerySingleAsync<T>(string sql, object? parameters = null)
    {
        await using var connection = await OpenAsync();
        return await connection.QuerySingleAsync<T>(sql, parameters);
    }

    public Task SeedAsync(int capacity = 1, bool open = true, string voyage = "voyage-1") =>
        ExecuteAsync("INSERT INTO voyage_capacity (voyage_id,total,reserved,confirmed,is_open) VALUES (@voyage,@capacity,0,0,@open)", new { voyage, capacity, open });

    public async Task<T> WithServiceAsync<TService, T>(Func<TService, Task<T>> action) where TService : notnull
    {
        await using var scope = Services.CreateAsyncScope();
        return await action(scope.ServiceProvider.GetRequiredService<TService>());
    }

    public async Task<HttpResponseMessage> CreateAsync(string booking = "booking-1", int quantity = 1,
        string? key = "create-1", string customer = "customer-1", string voyage = "voyage-1", HttpClient? client = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"/api/voyages/{voyage}/capacity-holds")
        {
            Content = JsonContent.Create(new CreateHoldRequest(booking, quantity))
        };
        request.Headers.Add("X-Customer-Id", customer);
        if (key is not null) request.Headers.Add("Idempotency-Key", key);
        return await (client ?? Client).SendAsync(request);
    }

    public async Task<HttpResponseMessage> ConfirmAsync(Guid hold, string? key = "confirm-1", string customer = "customer-1", HttpClient? client = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"/api/capacity-holds/{hold}/confirm");
        request.Headers.Add("X-Customer-Id", customer);
        if (key is not null) request.Headers.Add("Idempotency-Key", key);
        return await (client ?? Client).SendAsync(request);
    }

    public async Task<HttpResponseMessage> HoldRequestAsync(Guid hold, HttpMethod method, string customer = "customer-1")
    {
        using var request = new HttpRequestMessage(method, $"/api/capacity-holds/{hold}");
        request.Headers.Add("X-Customer-Id", customer);
        return await Client.SendAsync(request);
    }

    public async Task<HoldView> CreateHoldAsync(string booking = "booking-1", int quantity = 1, string key = "create-1", string voyage = "voyage-1")
    {
        using var response = await CreateAsync(booking, quantity, key, voyage: voyage);
        Assert.Equal(System.Net.HttpStatusCode.Created, response.StatusCode);
        return (await response.Content.ReadFromJsonAsync<HoldView>(JsonOptions))!;
    }

    public Task<int> ExpireDueAsync() => WithServiceAsync<IExpiryService, int>(service => service.ExpireDueAsync());
    public Task<bool> ExpireAsync(Guid hold) => WithServiceAsync<IExpiryService, bool>(service => service.ExpireAsync(hold));
    public Task<int> PublishAsync() => WithServiceAsync<IOutboxPublisher, int>(service => service.PublishBatchAsync());
    public Task<bool> ConsumeAsync(BookingConfirmedMessage message) =>
        WithServiceAsync<IBookingConfirmedConsumer, bool>(service => service.ConsumeAsync(message));

    public async Task<BookingConfirmedMessage> OutboxMessageAsync()
    {
        var payload = await ScalarAsync<string>("SELECT payload FROM outbox LIMIT 1");
        return JsonSerializer.Deserialize<BookingConfirmedMessage>(payload, JsonOptions)!;
    }

    public async Task WaitForLockWaitersAsync(int expected)
    {
        await WaitUntilAsync(async () => await ScalarAsync<int>(
            "SELECT count(*)::int FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND state='active'") >= expected,
            $"Expected at least {expected} independent PostgreSQL sessions blocked on real database locks.");
    }

    public static async Task WaitUntilAsync(Func<Task<bool>> predicate, string failure, TimeSpan? timeout = null)
    {
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < (timeout ?? TimeSpan.FromSeconds(15)))
        {
            if (await predicate()) return;
            await Task.Delay(20);
        }
        Assert.Fail(failure);
    }

    public async Task AssertAccountingAsync(int reserved, int confirmed, string voyage = "voyage-1")
    {
        var counters = await QuerySingleAsync<(int Reserved, int Confirmed)>(
            "SELECT reserved, confirmed FROM voyage_capacity WHERE voyage_id=@voyage", new { voyage });
        Assert.Equal(reserved, counters.Reserved);
        Assert.Equal(confirmed, counters.Confirmed);
        var active = await ScalarAsync<long>("SELECT coalesce(sum(quantity),0) FROM capacity_holds WHERE voyage_id=@voyage AND state='Active'", new { voyage });
        var consumed = await ScalarAsync<long>("SELECT coalesce(sum(quantity),0) FROM capacity_holds WHERE voyage_id=@voyage AND state='Consumed'", new { voyage });
        Assert.Equal(reserved, active);
        Assert.Equal(confirmed, consumed);
    }

    private sealed class TestApplication(string connectionString, TestObserver observer) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:Database"] = connectionString,
                ["Workers:Enabled"] = "false",
                ["Delivery:RetryBaseDelay"] = "00:00:00.010",
                ["Delivery:LeaseDuration"] = "00:00:01"
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

[Collection(DatabaseCollection.Name)]
public abstract class DatabaseTest(DatabaseFixture fixture) : IAsyncLifetime
{
    protected DatabaseFixture Db { get; } = fixture;
    public Task InitializeAsync() => Db.ResetAsync();
    public Task DisposeAsync() => Task.CompletedTask;
}
