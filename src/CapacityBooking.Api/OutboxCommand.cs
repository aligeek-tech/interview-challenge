using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using CapacityBooking.Application;

/// <summary>Local operator commands use database access; they do not expose a management HTTP endpoint.</summary>
public static class OutboxCommand
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    public static async Task<int> ExecuteAsync(IOutboxAdministration administration, IConfiguration configuration,
        bool redrive, TextWriter output, CancellationToken ct = default)
    {
        if (!redrive)
        {
            var rawLimit = configuration["limit"];
            var limit = 100;
            if (rawLimit is not null && (!int.TryParse(rawLimit, CultureInfo.InvariantCulture, out limit) || limit is < 1 or > 1000))
                return await InvalidAsync(output, "--limit must be between 1 and 1000.");
            var rows = await administration.ListQuarantinedAsync(limit, ct);
            await output.WriteLineAsync(JsonSerializer.Serialize(rows, JsonOptions));
            return 0;
        }

        if (!Guid.TryParse(configuration["message-id"], out var messageId) || messageId == Guid.Empty ||
            !Guid.TryParse(configuration["action-id"], out var actionId) || actionId == Guid.Empty ||
            !long.TryParse(configuration["version"], CultureInfo.InvariantCulture, out var version) || version < 0 ||
            string.IsNullOrWhiteSpace(configuration["actor"]) || string.IsNullOrWhiteSpace(configuration["reason"]))
            return await InvalidAsync(output, "Redrive requires --message-id, --version, --action-id, --actor and --reason.");

        var result = await administration.RedriveAsync(new(messageId, version, actionId,
            configuration["actor"]!, configuration["reason"]!), ct);
        await output.WriteLineAsync(JsonSerializer.Serialize(result, JsonOptions));
        return result.Status switch
        {
            OutboxRedriveStatus.Redriven => 0,
            OutboxRedriveStatus.InvalidRequest => 2,
            OutboxRedriveStatus.NotFound => 4,
            _ => 3
        };
    }

    private static async Task<int> InvalidAsync(TextWriter output, string message)
    {
        await output.WriteLineAsync(JsonSerializer.Serialize(new { status = "InvalidRequest", message }, JsonOptions));
        return 2;
    }
}
