using Dapper;
using Npgsql;

namespace CapacityBooking.Infrastructure;

public sealed class Database(string connectionString)
{
    static Database() => DefaultTypeMap.MatchNamesWithUnderscores = true;

    public async Task<NpgsqlConnection> OpenAsync(CancellationToken ct = default)
    {
        var connection = new NpgsqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(ct);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }
}
