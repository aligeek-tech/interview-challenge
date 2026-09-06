# ADR-001 — Capacity, Hold & Booking Consistency Boundaries

Status: Accepted · Date: 2026-09-06

## Context

Concurrent holds, confirmation, expiry and retries must preserve finite voyage capacity. A hold can be consumed once; a logical booking request can confirm once. Consumption, confirmation and integration intent must commit atomically within this single-application challenge.

## Decision

**One Booking Capacity bounded context, two aggregate roots, one local transaction.** `VoyageCapacity` owns capacity accounting and its `CapacityHold` entities; `Booking` owns logical request identity and confirmation. `CapacityUnits` and `HoldDeadline` are value objects. Dapper operations load counters and the target/relevant active hold, without materializing unbounded history.

The transaction deliberately spans both roots because their outcomes must be atomic. Aggregate ownership, transaction scope, database table and deployment unit are distinct decisions.

`BookingId` globally identifies the logical request. Its customer, voyage and quantity are fixed by the first accepted hold. Expired/cancelled holds may be replaced; confirmed bookings cannot create another. `Idempotency-Key` identifies an API operation, not the booking.

**PostgreSQL Read Committed uses this lock order:** idempotency claim for create/confirm → voyage capacity → booking → target/active hold. Only afterward does a separate `SELECT clock_timestamp()` establish the decision instant. Expiry reads candidate IDs without locks, then follows the same order; locking holds first would risk deadlocks.

An active hold confirms only when `decisionTime < expiresAt`; equality belongs to expiry. An admitted pre-deadline decision may commit afterward. Arriving or beginning a transaction earlier gives no priority. PostgreSQL `now()`/`CURRENT_TIMESTAMP` report transaction-start time, making them unsuitable after a lock wait. [Database clock semantics](https://www.postgresql.org/docs/current/functions-datetime.html)

Creation reserves units; consumption transfers reserved to confirmed; expiry/cancellation releases reserved units once. Terminal states cannot reopen. Late DELETE expires the hold; consumed holds cannot cancel. Voyage closure blocks new holds while preserving existing confirmation windows.

Database protections include nonnegative counters, overflow-safe `reserved + confirmed <= total`, positive quantities, partial active-hold uniqueness, unique booking/confirmed-hold identities and composite foreign keys. Tests also reconcile counters against hold/booking rows.

Expiry persists in deadlines/state. Due-but-unreconciled holds conservatively retain capacity. Confirmation lazily expires a due active hold; replacement creation expires/releases its prior due hold before inserting another. GET may project elapsed state without writing.

Create/confirm claims are unique by customer, operation-with-route and key, with a typed-request fingerprint and stored response. Claim, business state, audit and outbox commit together. Completed business rejections replay; transient failures roll back. A deferred constraint rejects unfinished committed claims. Different keys cannot bypass booking uniqueness.

Lifecycle/rejection facts persist in audit; `DuplicateCommandDetected` is operational logging. Only `BookingConfirmed` becomes an integration message. Publication and the downstream inbox/projection transaction occur later; see ADR-002.

## Alternatives

| Alternative | Trade-off |
|---|---|
| Optimistic versions | Correct with transaction retries; hot-voyage conflicts repeat work. |
| Atomic conditional UPDATE | Valid, but associated hold/booking changes still need a transaction; ordered locks expose the deadline decision clearly. |
| One aggregate containing every booking | Blurs request identity and capacity ownership; encourages unbounded history loading. |
| Independent services plus saga | Distributes an immediate atomicity requirement without a present ownership/deployment need. |
| In-memory/distributed lock alone | Cannot replace durable constraints and transaction recovery. |

## Consequences

Twenty requests for the final unit produce one allocation and replayable conflicts for the others. Transactions contain no broker/payment calls. Hot-voyage writes serialize; extra instances do not remove that bottleneck. Short transactions, controlled admission and bounded retries matter. Expiry lag understates availability. The deliberate multi-aggregate coupling cannot retain its atomic guarantee unchanged after splitting stores.

## Revisit when

- Measured contention exceeds service levels despite admission control: investigate allocation partitioning/escrow with a new invariant proof.
- Business ownership requires separate stores: redesign consistency and coordination explicitly.
- Payment authorization arrives: introduce a durable process manager outside the short local transaction.
- Logical identity, deadline priority or closure rules change: update the model, constraints and race tests together.
