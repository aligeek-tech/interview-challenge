# Failure sequence: deadline race and duplicate publication

[Open the rendered diagram at full size](failure-sequence.svg). The Mermaid source below remains editable.

The first section shows both operations beginning before either business transaction commits. The confirmation decision uses a separate `clock_timestamp()` read **after** the voyage, booking and hold locks are acquired. Confirmation succeeds only for Active state with `decisionTime < expiresAt`; equality expires. A valid decision may commit after the deadline. The winner is defined by serialized state and this time check, not by HTTP arrival time.

```mermaid
sequenceDiagram
    autonumber
    participant C as Confirm request
    participant E as Expiry worker
    participant D as PostgreSQL
    participant P as Outbox publisher
    participant T as Local transport
    participant K as Downstream consumer

    C->>D: BEGIN and claim idempotency key
    E->>D: Read candidate identity, BEGIN
    alt Confirmation obtains locks and decides before deadline
        C->>D: Lock voyage, then booking, then hold
        E->>D: Request voyage lock, wait
        C->>D: Read clock_timestamp(), Active and now < expiresAt
        C->>D: Consume hold, confirm booking, update counters
        C->>D: Write audit, outbox M1 and idempotent response
        C->>D: COMMIT
        D-->>E: Acquire voyage lock after confirmation commits
        E->>D: Lock booking and hold, then read DB time
        E->>D: Observe Consumed, COMMIT no transition
    else Expiry obtains locks at or after deadline
        E->>D: Lock voyage, then booking, then hold
        C->>D: Request voyage lock, wait
        E->>D: Read clock_timestamp(), Active and now >= expiresAt
        E->>D: Expire hold, release reserved capacity, write audit
        E->>D: COMMIT
        D-->>C: Acquire voyage lock after expiry commits
        C->>D: Lock booking and hold, then read DB time
        C->>D: Observe Expired, persist rejection response, COMMIT
        Note over C,D: No booking confirmation or BookingConfirmed outbox event
    end

    opt Confirmation transaction committed
        P->>D: Atomically claim M1 with lease token L1
        D-->>P: Claim committed, connection released
        P->>T: Publish original envelope M1
        T->>K: Consume M1
        K->>D: BEGIN, insert inbox M1 and projection
        K->>D: COMMIT inbox and downstream effect
        K-->>T: Effective result committed
        T-->>P: Acknowledge delivery
        Note over P,D: Publisher process dies before marking M1 published

        Note over P: A new publisher process starts
        P->>D: After lease expiry, claim M1 with token L2
        D-->>P: Same MessageId and payload, new attempt
        P->>T: Publish M1 again
        T->>K: Consume duplicate M1
        K->>D: BEGIN, inbox insert conflicts with committed M1
        K->>D: COMMIT without another projection write
        K-->>T: Duplicate safely acknowledged
        T-->>P: Acknowledge delivery
        P->>D: Mark published only WHERE lease_token = L2
        Note over D,K: One effective downstream result, repeated delivery is allowed
    end
```

If confirmation acquires the locks after the deadline while the hold still says Active, it performs the same expiry/release transition lazily and returns a rejection. If an expiry call acquires them before the deadline, it makes no transition. These cases obey the same decision rule. PostgreSQL holds the conflicting row locks to transaction end, so a waiting operation subsequently observes the committed state. [PostgreSQL row-level locking](https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS).

The publication section depicts the **implemented local transport**: acknowledgement follows the consumer's separate database commit. With the proposed RabbitMQ adapter, publisher confirmation establishes broker acceptance and the consumer acknowledges independently after its transaction. The repeated-MessageId/inbox protection is unchanged. [RabbitMQ confirmation boundaries](https://www.rabbitmq.com/docs/confirms).

The failure seams are `business.after-locks`, `confirm.after-decision`, `confirm.before-commit`, `consumer.before-commit` and `outbox.after-publish`. Integration tests inject gates or exceptions through internal DI; process recovery demonstrations terminate and relaunch the application against retained database state. A handled publication exception schedules a retry; a dead process leaves its lease to expire.
