using Dapper;
using Npgsql;
using Xunit;

namespace CapacityBooking.IntegrationTests;

public sealed class DatabaseInvariantTests(DatabaseFixture fixture) : DatabaseTest(fixture)
{
    [Fact]
    public async Task DatabaseCannotCommitAnUnfinishedIdempotencyClaim()
    {
        await using var connection = await Db.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await connection.ExecuteAsync("""
            INSERT INTO idempotency_records(customer_id,operation,idempotency_key,fingerprint)
            VALUES ('customer','operation','incomplete-key','fingerprint')
            """, transaction: transaction);
        var error = await Assert.ThrowsAsync<PostgresException>(() => transaction.CommitAsync());
        Assert.Equal(PostgresErrorCodes.CheckViolation, error.SqlState);
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM idempotency_records"));
    }

    [Fact]
    public async Task DatabaseRejectsASecondActiveHoldForTheSameBooking()
    {
        await Db.SeedAsync(10);
        await Db.CreateHoldAsync();
        var error = await Assert.ThrowsAsync<PostgresException>(() => Db.ExecuteAsync("""
            INSERT INTO capacity_holds(hold_id,booking_id,voyage_id,quantity,state,created_at,expires_at)
            SELECT @secondId,booking_id,voyage_id,quantity,'Active',created_at,expires_at
            FROM capacity_holds
            """, new { secondId = Guid.NewGuid() }));
        Assert.Equal(PostgresErrorCodes.UniqueViolation, error.SqlState);
        Assert.Equal("one_active_hold_per_booking_voyage", error.ConstraintName);
        Assert.Equal(1, await Db.ScalarAsync<int>("SELECT count(*)::int FROM capacity_holds"));
        await Db.AssertAccountingAsync(1, 0);
    }

    [Fact]
    public async Task DatabaseRejectsConfirmationUsingAnotherBookingsHold()
    {
        await Db.SeedAsync(2);
        var first = await Db.CreateHoldAsync();
        await Db.CreateHoldAsync(booking: "booking-2", key: "create-2");
        var error = await Assert.ThrowsAsync<PostgresException>(() => Db.ExecuteAsync("""
            UPDATE bookings SET confirmed_hold_id=@foreignHold, confirmed_at=clock_timestamp()
            WHERE booking_id='booking-2'
            """, new { foreignHold = first.HoldId }));
        Assert.Equal(PostgresErrorCodes.ForeignKeyViolation, error.SqlState);
        Assert.Equal("booking_confirmed_hold_belongs_to_booking", error.ConstraintName);
        Assert.Equal(0, await Db.ScalarAsync<int>("SELECT count(*)::int FROM bookings WHERE confirmed_hold_id IS NOT NULL"));
        await Db.AssertAccountingAsync(2, 0);
    }
}
