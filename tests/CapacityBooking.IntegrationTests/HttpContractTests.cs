using System.Net;
using System.Text;
using System.Text.Json;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class HttpContractTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Theory]
    [InlineData("null")]
    [InlineData("{malformed")]
    [InlineData("{\"bookingId\":null,\"quantity\":1}")]
    public async Task InvalidJsonBodiesReturnClientErrorsWithoutBusinessEffects(string body)
    {
        await Db.SeedAsync();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/voyages/voyage-1/capacity-holds")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };
        request.Headers.Add("X-Customer-Id", "customer-1");
        request.Headers.Add("Idempotency-Key", "invalid-body");
        using var response = await Db.Client.SendAsync(request);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(0, 0);
    }

    [Fact]
    public async Task OversizedRequestIsRejectedByTheActualHttpServer()
    {
        await Db.SeedAsync();
        await using var server = await ApiProcess.StartAsync(Db.ConnectionString, workers: false);
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/voyages/voyage-1/capacity-holds")
        {
            Content = new StringContent(JsonSerializer.Serialize(new { bookingId = "booking-1", quantity = 1, padding = new string('x', 20_000) }), Encoding.UTF8, "application/json")
        };
        request.Headers.Add("X-Customer-Id", "customer-1");
        request.Headers.Add("Idempotency-Key", "oversized-body");
        using var response = await server.Client.SendAsync(request);
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(0, 0);
    }
}
