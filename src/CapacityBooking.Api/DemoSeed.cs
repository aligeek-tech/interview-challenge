using System.Text.Json;
using CapacityBooking.Application;
using CapacityBooking.Infrastructure;
using Dapper;

public static class DemoSeed
{
    public static async Task RunAsync(IServiceProvider services)
    {
        await using (var connection = await services.GetRequiredService<Database>().OpenAsync())
        {
            await connection.ExecuteAsync("""
                INSERT INTO voyage_capacity(voyage_id,total,is_open) VALUES
                    ('VYG-1001',100,true),('VYG-LAST',1,true),('VYG-CLOSED',100,false)
                ON CONFLICT(voyage_id) DO NOTHING
                """);
            var alreadyConfirmed = await connection.ExecuteScalarAsync<bool>("""
                SELECT EXISTS(SELECT 1 FROM bookings WHERE booking_id='BKG-BASELINE' AND confirmed_at IS NOT NULL)
                """);
            if (alreadyConfirmed) return;
        }
        var bookings = services.GetRequiredService<IBookingService>();
        // A new seed attempt can replace a previously expired hold after interrupted setup.
        var attempt = Guid.NewGuid().ToString("N");
        var context = new RequestContext("demo-customer", "demo-seed");
        var hold = await bookings.CreateHoldAsync("VYG-1001", new("BKG-BASELINE", 97), $"seed-{attempt}", context);
        if (hold.StatusCode != 201)
            throw new InvalidOperationException("Seed baseline could not reserve capacity. Run against a fresh demo database.");
        using var json = JsonDocument.Parse(hold.Json);
        var holdId = json.RootElement.GetProperty("holdId").GetGuid();
        var confirmation = await bookings.ConfirmAsync(holdId, $"seed-confirm-{attempt}", context);
        if (confirmation.StatusCode != 200)
            throw new InvalidOperationException("Seed baseline could not confirm capacity.");
    }
}
