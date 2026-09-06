using System.Diagnostics;
using System.Text.Json;
using CapacityBooking.Application;
using CapacityBooking.Infrastructure;
using CapacityBooking.Infrastructure.Reliability;
using Dapper;
using Npgsql;

var migrate = args.Contains("--migrate", StringComparer.Ordinal);
var seed = args.Contains("--seed", StringComparer.Ordinal);
var builder = WebApplication.CreateBuilder(args.Where(a => a is not "--migrate" and not "--seed").ToArray());
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole(options => options.IncludeScopes = true);
builder.Services.Configure<BookingOptions>(builder.Configuration.GetSection("Booking"));
builder.Services.Configure<DeliveryOptions>(builder.Configuration.GetSection("Delivery"));
builder.Services.Configure<RouteHandlerOptions>(options => options.ThrowOnBadRequest = true);
builder.Services.AddSingleton(provider =>
{
    var connectionString = provider.GetRequiredService<IConfiguration>().GetConnectionString("Database")
        ?? throw new InvalidOperationException("Set ConnectionStrings__Database before starting the application.");
    return new Database(connectionString);
});
builder.Services.AddSingleton<MigrationRunner>();
builder.Services.AddSingleton<IExecutionObserver, ProcessExecutionObserver>();
builder.Services.AddScoped<IBookingService, BookingService>();
builder.Services.AddScoped<IExpiryService, ExpiryService>();
builder.Services.AddScoped<IBookingConfirmedConsumer, BookingConfirmedConsumer>();
builder.Services.AddScoped<LocalMessageTransport>();
builder.Services.AddScoped<IMessageTransport, DemonstrationTransport>();
builder.Services.AddScoped<IOutboxPublisher, OutboxPublisher>();
builder.Services.AddHostedService<ExpiryWorker>();
builder.Services.AddHostedService<OutboxWorker>();
builder.WebHost.ConfigureKestrel(options => options.Limits.MaxRequestBodySize = 16 * 1024);

var app = builder.Build();
if (!app.Environment.IsDevelopment() &&
    (!string.IsNullOrWhiteSpace(app.Configuration["FailureInjection:Point"]) ||
     app.Configuration.GetValue<bool>("DemoTransport:Unavailable")))
    throw new InvalidOperationException("Failure simulation is permitted only in Development.");

if (migrate || seed)
{
    await app.Services.GetRequiredService<MigrationRunner>().MigrateAsync();
    if (seed)
    {
        await using var scope = app.Services.CreateAsyncScope();
        await DemoSeed.RunAsync(scope.ServiceProvider);
    }
    return;
}

if (!app.Environment.IsDevelopment() && !app.Environment.IsEnvironment("Testing"))
    throw new InvalidOperationException(
        "This challenge uses a demonstration identity header. Serve it only in Development or Testing; " +
        "a production host requires a real authentication adapter.");

app.Use(async (context, next) =>
{
    var traceId = Activity.Current?.TraceId.ToString() ?? context.TraceIdentifier;
    context.Response.Headers["X-Trace-Id"] = traceId;
    using var logScope = app.Logger.BeginScope(new Dictionary<string, object> { ["TraceId"] = traceId });
    try
    {
        if (context.Request.Path.StartsWithSegments("/api"))
        {
            var customerHeader = context.Request.Headers["X-Customer-Id"];
            var customer = customerHeader.ToString();
            if (customerHeader.Count != 1 || customer.Length is < 1 or > 128 ||
                customer.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '-' and not '_' and not '.'))
            {
                context.Response.StatusCode = 401;
                await context.Response.WriteAsJsonAsync(new ApiError("IDENTITY_REQUIRED",
                    "Provide the demonstration X-Customer-Id header."), context.RequestAborted);
                return;
            }
            context.Items["RequestContext"] = new RequestContext(customer, traceId);
        }
        await next(context);
    }
    catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
    {
        // A lost response does not reverse an already committed operation; the same key can replay it.
        if (!context.Response.HasStarted) context.Response.StatusCode = 499;
    }
    catch (NpgsqlException error) when (error.IsTransient || error.SqlState is "55P03" or "57014" or "40P01" or "40001")
    {
        app.Logger.LogWarning("Database operation could not complete; SqlState={SqlState}", error.SqlState);
        if (context.Response.HasStarted) throw;
        context.Response.StatusCode = 503;
        context.Response.Headers.RetryAfter = "1";
        await context.Response.WriteAsJsonAsync(new ApiError("TEMPORARILY_UNAVAILABLE",
            "Retry the same request with the same idempotency key."));
    }
    catch (BadHttpRequestException error)
    {
        if (context.Response.HasStarted) throw;
        context.Response.StatusCode = error.StatusCode;
        await context.Response.WriteAsJsonAsync(new ApiError("INVALID_REQUEST", "The request is invalid."));
    }
    catch (Exception error)
    {
        app.Logger.LogError("Request failed; ErrorType={ErrorType}", error.GetType().Name);
        if (context.Response.HasStarted) throw;
        context.Response.StatusCode = 500;
        await context.Response.WriteAsJsonAsync(new ApiError("INTERNAL_ERROR", "The operation could not complete."));
    }
});

app.MapPost("/api/voyages/{voyageId}/capacity-holds", async (string voyageId, CreateHoldRequest request,
    HttpContext http, IBookingService bookings, CancellationToken ct) =>
{
    var key = ReadKey(http);
    return key is null ? MissingKey() : WriteResult(http,
        await bookings.CreateHoldAsync(voyageId, request, key, Identity(http), ct));
});
app.MapPost("/api/capacity-holds/{holdId:guid}/confirm", async (Guid holdId, HttpContext http,
    IBookingService bookings, CancellationToken ct) =>
{
    var key = ReadKey(http);
    return key is null ? MissingKey() : WriteResult(http,
        await bookings.ConfirmAsync(holdId, key, Identity(http), ct));
});
app.MapDelete("/api/capacity-holds/{holdId:guid}", async (Guid holdId, HttpContext http,
    IBookingService bookings, CancellationToken ct) =>
    WriteResult(http, await bookings.CancelAsync(holdId, Identity(http), ct)));
app.MapGet("/api/capacity-holds/{holdId:guid}", async (Guid holdId, HttpContext http,
    IBookingService bookings, CancellationToken ct) =>
    WriteResult(http, await bookings.GetAsync(holdId, Identity(http), ct)));
app.MapGet("/health", async (Database database, CancellationToken ct) =>
{
    try
    {
        await using var connection = await database.OpenAsync(ct);
        // Readiness includes applied schema; accepting TCP alone is insufficient.
        await connection.ExecuteScalarAsync<int>(new CommandDefinition("SELECT count(*)::integer FROM schema_migrations", cancellationToken: ct));
        return Results.Ok(new { status = "healthy" });
    }
    catch (Exception error) when (error is NpgsqlException or TimeoutException)
    {
        return Results.Json(new { status = "unhealthy" }, statusCode: 503);
    }
});
await app.RunAsync();

static RequestContext Identity(HttpContext http) => (RequestContext)http.Items["RequestContext"]!;
static string? ReadKey(HttpContext http)
{
    var values = http.Request.Headers["Idempotency-Key"];
    var key = values.ToString();
    return values.Count == 1 && key.Length is >= 1 and <= 128 &&
        key.All(c => c is >= '!' and <= '~') ? key : null;
}
static IResult MissingKey() => Results.Json(new ApiError("IDEMPOTENCY_KEY_REQUIRED",
    "Provide one Idempotency-Key containing 1 to 128 visible ASCII characters."), statusCode: 400);
static IResult WriteResult(HttpContext http, OperationResult result)
{
    if (result.Replayed) http.Response.Headers["Idempotency-Replayed"] = "true";
    if (result.StatusCode == 201)
    {
        using var body = JsonDocument.Parse(result.Json);
        if (body.RootElement.TryGetProperty("holdId", out var id))
            http.Response.Headers.Location = $"/api/capacity-holds/{id.GetString()}";
    }
    return Results.Content(result.Json, "application/json", statusCode: result.StatusCode);
}

public partial class Program;
