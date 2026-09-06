using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class ProcessRecoveryTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Fact]
    public async Task ProductionProcessRefusesToServeWithTheDemonstrationIdentityStub()
    {
        await using var production = await ApiProcess.StartAsync(Db.ConnectionString, workers: false,
            waitForHealthy: false, environment: "Production");
        var exitCode = await production.WaitForExitAsync();
        Assert.NotEqual(0, exitCode);
        Assert.NotEqual(86, exitCode);
        Assert.Contains("authentication", production.CapturedOutput, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ProcessFailpointsAreRejectedOutsideDevelopment()
    {
        await using var testing = await ApiProcess.StartAsync(Db.ConnectionString, workers: false,
            failpoint: "confirm.before-commit", waitForHealthy: false, environment: "Testing");
        var exitCode = await testing.WaitForExitAsync();
        Assert.NotEqual(0, exitCode);
        Assert.NotEqual(86, exitCode);
        Assert.Contains("Failure simulation is permitted only in Development", testing.CapturedOutput, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FreshOperatingSystemProcessExpiresHoldsPersistedBeforeShutdown()
    {
        await Db.SeedAsync();
        Guid holdId;
        int firstPid;
        await using (var original = await ApiProcess.StartAsync(Db.ConnectionString, workers: false, holdTtl: TimeSpan.FromSeconds(2)))
        {
            firstPid = original.Id;
            using var response = await Db.CreateAsync(client: original.Client);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
            holdId = await Db.ScalarAsync<Guid>("SELECT hold_id FROM capacity_holds");
            original.Kill();
            await original.WaitForExitAsync();
        }

        Assert.Equal("Active", await Db.ScalarAsync<string>("SELECT state FROM capacity_holds WHERE hold_id=@holdId", new { holdId }));
        await Db.AssertAccountingAsync(1, 0);
        // The original persisted deadline elapses while the application process is absent.
        await DatabaseFixture.WaitUntilAsync(
            () => Db.ScalarAsync<bool>("SELECT clock_timestamp()>=expires_at FROM capacity_holds WHERE hold_id=@holdId", new { holdId }),
            "The original hold deadline did not elapse during application downtime.");
        await using var restarted = await ApiProcess.StartAsync(Db.ConnectionString, workers: true);
        Assert.NotEqual(firstPid, restarted.Id);
        await DatabaseFixture.WaitUntilAsync(
            async () => await Db.ScalarAsync<string>("SELECT state FROM capacity_holds WHERE hold_id=@holdId", new { holdId }) == "Expired",
            "A fresh API process did not reconcile the hold persisted before shutdown.");
        await Db.AssertAccountingAsync(0, 0);
    }

    [Fact]
    public async Task ProcessDeathAfterPublishLeavesLeaseAndRestartSafelyRedelivers()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        using var confirm = await Db.ConfirmAsync(hold.HoldId);
        Assert.Equal(HttpStatusCode.OK, confirm.StatusCode);
        var originalMessage = await Db.OutboxMessageAsync();
        await using (var crashing = await ApiProcess.StartAsync(Db.ConnectionString, workers: true,
            failpoint: "outbox.after-publish", waitForHealthy: false))
        {
            Assert.Equal(86, await crashing.WaitForExitAsync());
        }

        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE published_at IS NULL AND lease_token IS NOT NULL"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
        await using var restarted = await ApiProcess.StartAsync(Db.ConnectionString, workers: true);
        await DatabaseFixture.WaitUntilAsync(
            async () => await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE published_at IS NOT NULL") == 1,
            "The restarted publisher did not reclaim and publish the abandoned outbox lease.");
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
        Assert.Equal(originalMessage.MessageId, await Db.ScalarAsync<Guid>("SELECT message_id FROM booking_confirmations"));
        Assert.True(await Db.ScalarAsync<int>("SELECT attempts FROM outbox") >= 2);
        await Db.AssertAccountingAsync(0, 1);
    }

    [Fact]
    public async Task ProcessDeathBeforeConfirmationCommitRollsBackAndSameKeyCanRetry()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        await using (var crashing = await ApiProcess.StartAsync(Db.ConnectionString, workers: false,
            failpoint: "confirm.before-commit"))
        {
            // The HTTP connection may close with any transport exception when the process exits.
            try { using var ignored = await Db.ConfirmAsync(hold.HoldId, "crash-confirm", client: crashing.Client); }
            catch (HttpRequestException) { }
            Assert.Equal(86, await crashing.WaitForExitAsync());
        }
        await Db.AssertAccountingAsync(1, 0);
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM bookings WHERE confirmed_hold_id IS NOT NULL"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM idempotency_records WHERE idempotency_key='crash-confirm'"));
        await using var restarted = await ApiProcess.StartAsync(Db.ConnectionString, workers: false);
        using var retry = await Db.ConfirmAsync(hold.HoldId, "crash-confirm", client: restarted.Client);
        Assert.Equal(HttpStatusCode.OK, retry.StatusCode);
        await Db.AssertAccountingAsync(0, 1);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox"));
    }

    [Fact]
    public async Task TransportOutageAcrossProcessRestartPreservesConfirmationAndRecoversPublication()
    {
        await Db.SeedAsync();
        var hold = await Db.CreateHoldAsync();
        using var confirmation = await Db.ConfirmAsync(hold.HoldId);
        Assert.Equal(HttpStatusCode.OK, confirmation.StatusCode);
        await using (var unavailable = await ApiProcess.StartAsync(Db.ConnectionString, workers: true, unavailableTransport: true))
        {
            await DatabaseFixture.WaitUntilAsync(
                async () => await Db.ScalarAsync<int>("SELECT attempts FROM outbox") >= 1,
                "Publisher did not attempt the unavailable transport.");
        }
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE published_at IS NULL"));
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM inbox"));
        await Db.AssertAccountingAsync(0, 1);
        await using var recovered = await ApiProcess.StartAsync(Db.ConnectionString, workers: true);
        await DatabaseFixture.WaitUntilAsync(
            async () => await Db.ScalarAsync<int>("SELECT count(*)::int FROM outbox WHERE published_at IS NOT NULL") == 1,
            "Recovered transport did not publish the durable event.");
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM booking_confirmations"));
    }
}

internal sealed class ApiProcess : IAsyncDisposable
{
    private readonly Process _process;
    private readonly StringBuilder _output = new();
    public HttpClient Client { get; }
    public int Id => _process.Id;

    private ApiProcess(Process process, Uri baseAddress)
    {
        _process = process;
        Client = new HttpClient { BaseAddress = baseAddress, Timeout = TimeSpan.FromSeconds(15) };
        process.OutputDataReceived += (_, args) => Append(args.Data);
        process.ErrorDataReceived += (_, args) => Append(args.Data);
    }

    public static async Task<ApiProcess> StartAsync(string database, bool workers, string? failpoint = null,
        bool waitForHealthy = true, bool unavailableTransport = false, string environment = "Development", TimeSpan? holdTtl = null)
    {
        var root = FindRepositoryRoot();
        var configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent!.Name;
        var assembly = Path.Combine(root, "src", "CapacityBooking.Api", "bin", configuration, "net10.0", "CapacityBooking.Api.dll");
        if (!File.Exists(assembly)) throw new FileNotFoundException("Build the API project before running process recovery tests.", assembly);
        var localDotnet = Path.Combine(root, ".tools", "dotnet", "dotnet");
        var runtimeDotnet = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(typeof(object).Assembly.Location)!, "../../..", "dotnet"));
        var executable = File.Exists(localDotnet) ? localDotnet :
            Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? (File.Exists(runtimeDotnet) ? runtimeDotnet : "dotnet");
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        var baseAddress = new Uri($"http://127.0.0.1:{port}");
        var start = new ProcessStartInfo(executable)
        {
            WorkingDirectory = Path.Combine(root, "src", "CapacityBooking.Api"),
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        start.ArgumentList.Add(assembly);
        start.Environment["ASPNETCORE_ENVIRONMENT"] = environment;
        start.Environment["ASPNETCORE_URLS"] = baseAddress.ToString();
        start.Environment["ConnectionStrings__Database"] = database;
        start.Environment["Workers__Enabled"] = workers.ToString();
        start.Environment["Workers__PollIntervalMilliseconds"] = "50";
        start.Environment["Delivery__RetryBaseDelay"] = "00:00:00.100";
        start.Environment["Delivery__LeaseDuration"] = "00:00:01";
        start.Environment["Booking__HoldTtl"] = (holdTtl ?? TimeSpan.FromMinutes(2)).ToString("c");
        start.Environment["Logging__LogLevel__Default"] = "Warning";
        start.Environment["FailureInjection__Point"] = failpoint ?? "";
        start.Environment["DemoTransport__Unavailable"] = unavailableTransport.ToString();
        var process = new Process { StartInfo = start, EnableRaisingEvents = true };
        var instance = new ApiProcess(process, baseAddress);
        try
        {
            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            if (waitForHealthy) await instance.WaitForHealthyAsync();
            return instance;
        }
        catch
        {
            await instance.DisposeAsync();
            throw;
        }
    }

    private void Append(string? value)
    {
        if (value is null) return;
        lock (_output)
        {
            if (_output.Length < 20_000) _output.AppendLine(value);
        }
    }

    public string CapturedOutput { get { lock (_output) return _output.ToString(); } }

    private async Task WaitForHealthyAsync()
    {
        var timeout = Stopwatch.StartNew();
        while (timeout.Elapsed < TimeSpan.FromSeconds(20))
        {
            if (_process.HasExited) throw new InvalidOperationException($"API process exited {_process.ExitCode} before readiness. {CapturedOutput}");
            try
            {
                using var response = await Client.GetAsync("/health");
                if (response.IsSuccessStatusCode) return;
            }
            catch (HttpRequestException) { }
            await Task.Delay(30);
        }
        throw new TimeoutException($"API process failed to become healthy. {CapturedOutput}");
    }

    public async Task<int> WaitForExitAsync()
    {
        try { await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(20)); }
        catch (TimeoutException error) { throw new TimeoutException($"API process did not exit. {CapturedOutput}", error); }
        return _process.ExitCode;
    }

    public void Kill()
    {
        if (!_process.HasExited) _process.Kill(entireProcessTree: true);
    }

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        try
        {
            Kill();
            await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally { _process.Dispose(); }
    }

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "src", "CapacityBooking.Api", "CapacityBooking.Api.csproj"))) return directory.FullName;
            directory = directory.Parent;
        }
        throw new DirectoryNotFoundException("Cannot locate the source repository for operating-system process recovery tests.");
    }
}
