namespace CapacityBooking.Application;

public enum DeliveryFailureKind { Transient, Permanent, Unknown }

/// <summary>An adapter may explicitly classify a known delivery failure without leaking its payload.</summary>
public sealed class DeliveryFailureException : Exception
{
    public DeliveryFailureKind Kind { get; }
    public string ReasonCode { get; }

    public DeliveryFailureException(DeliveryFailureKind kind, string reasonCode)
        : base("Message delivery failed; inspect the classified reason code.")
    {
        if (!Enum.IsDefined(kind) || string.IsNullOrWhiteSpace(reasonCode) || reasonCode.Length > 64 ||
            reasonCode.Any(character => !char.IsAsciiLetterOrDigit(character) && character != '_'))
            throw new ArgumentException("A delivery failure requires a bounded classification code.");
        Kind = kind;
        ReasonCode = reasonCode;
    }
}
