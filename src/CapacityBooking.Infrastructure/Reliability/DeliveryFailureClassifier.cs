using CapacityBooking.Application;
using Npgsql;

namespace CapacityBooking.Infrastructure.Reliability;

public sealed record ClassifiedDeliveryFailure(DeliveryFailureKind Kind, string ReasonCode);

public static class DeliveryFailureClassifier
{
    public static ClassifiedDeliveryFailure Classify(Exception exception) => exception switch
    {
        DeliveryFailureException failure => new(failure.Kind, failure.ReasonCode),
        PostgresException conflict when conflict.SqlState == PostgresErrorCodes.UniqueViolation &&
            conflict.TableName == "booking_confirmations" && conflict.ConstraintName is
                "booking_confirmations_pkey" or "booking_confirmations_hold_id_key" or "booking_confirmations_message_id_key" =>
            new(DeliveryFailureKind.Permanent, "DOWNSTREAM_IDENTITY_CONFLICT"),
        NpgsqlException databaseFailure when databaseFailure.IsTransient =>
            new(DeliveryFailureKind.Transient, "DATABASE_TEMPORARILY_UNAVAILABLE"),
        TimeoutException => new(DeliveryFailureKind.Transient, "DELIVERY_TIMEOUT"),
        IOException => new(DeliveryFailureKind.Transient, "TRANSPORT_IO_FAILURE"),
        _ => new(DeliveryFailureKind.Unknown, "UNCLASSIFIED_DELIVERY_FAILURE")
    };
}
