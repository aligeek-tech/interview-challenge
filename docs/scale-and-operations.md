# Scale and operations

The 5,000 hold requests/second scenario is a design input, not a measured result. The executable prioritizes durable correctness and reproducible failure tests. No 5,000 requests/second benchmark, production capacity claim or real-broker performance test is included.

## Capacity and traffic

| Concern | Expected pressure and production response |
|---|---|
| Hot-key contention | All mutations for one voyage serialize on its capacity row. PostgreSQL keeps conflicting row writers/lockers waiting until the transaction ends. Keep this critical section short and measure lock wait separately from lock holding time. More API replicas cannot remove this serial constraint. [PostgreSQL row locks](https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS). |
| Partitioning / routing | Route or shard by VoyageId if measured load warrants it; keep the voyage, related booking/hold mutations and their outbox transaction colocated. Routing improves locality and permits admission control, but one hot voyage remains one coordination point. Splitting its capacity into allocations requires a new, explicit invariant design. |
| Database write contention | Successful requests write capacity, hold/booking, idempotency and audit records; confirmation also writes outbox. Rejected requests may still persist idempotent responses. Measure WAL volume, commit latency, index maintenance and vacuum pressure. Broker I/O is already outside business transactions. |
| Retry amplification | Clients reuse the same key after uncertain outcomes. Expected capacity/state rejections are terminal for that key; retrying them automatically cannot create capacity. Production clients need bounded exponential backoff with jitter for transient faults, retry budgets and cancellation. The outbox already uses bounded exponential delay; client retry policy and jitter are not implemented. |
| Backpressure | Add bounded in-flight requests and per-customer/per-voyage admission, returning a documented retry response before pools and lock queues saturate. Reserve operating headroom for confirmation and expiry so a burst of new holds cannot consume all resources. These admission policies are design recommendations. |
| Connection pool pressure | Size the total connection budget across all instances, HTTP requests and workers against database capacity. Monitor pool wait and active connections. Unbounded asynchronous requests still consume finite database sessions. The publisher releases its claim connection before transport I/O; waiting business transactions still occupy sessions. |
| Outbox backlog | Track count, bytes and oldest age, not just successful publishes. The partial pending index and short independent claims allow several publishers; each currently publishes sequentially within its batch. Add bounded publisher concurrency only after measuring DB/broker/consumer limits. Slow local consumers directly limit the demonstration publisher. |
| Horizontal scale limits | Additional instances help independent voyages and independent outbox messages. They can also increase duplicate expiry scans, DB connections and hot-row waits. First optimize/measure the single voyage transaction; then consider query batching, worker partitioning or explicit ownership. Do not replace durable correctness with a process-local lock. |

For a voyage whose measured lock holding time is `s` seconds, `1/s` is only a rough upper bound on serialized transactions/second before other costs. It is not a throughput estimate for this implementation. Each success, rejection, cancellation, expiry and confirmation may compete for that same lock.

## Index and uniqueness rationale

| Storage concept | Implemented protection/access path |
|---|---|
| VoyageCapacity | Primary key on VoyageId locates the coordination row; checks prohibit negative counters and capacity overselling. |
| CapacityHold | HoldId primary key; partial unique `(booking_id, voyage_id)` for Active holds; partial `(expires_at, hold_id)` index for durable due work; composite identity supports the booking/hold foreign-key relationship. |
| Booking | BookingId primary key binds the logical request; unique confirmed HoldId and composite foreign key prevent reusing or attaching another booking's hold. |
| IdempotencyRecord | Composite primary key `(customer_id, operation, idempotency_key)` arbitrates cross-instance duplicates. Fingerprint detects changed payload. A deferred constraint rejects incomplete claims at commit. |
| Outbox | MessageId primary key; unique `(event_type, aggregate_id)` protects the once-only confirmation event; partial pending index begins with next attempt time for due claims. Lease tokens guard outcomes. |
| Inbox | Composite primary key `(consumer_name, message_id)` arbitrates concurrent duplicate deliveries. The projection separately has BookingId, HoldId and MessageId uniqueness. |

Due-work selection uses a stable database statement timestamp for indexed eligibility; expiry then samples the actual database clock after all business locks before deciding. The publisher uses `SKIP LOCKED` only for outbox claims. Expiry follows voyage → booking → hold lock order and never claims a hold first.

## Measurements needed to prove the design

Structured logs, business audit rows and the database health endpoint form the implemented baseline. The following names are a **production instrumentation plan**, not a claim that a metrics exporter is installed:

| Metric | Purpose |
|---|---|
| `capacity_hold_created_total`, `capacity_hold_rejected_total`, `capacity_hold_expired_total` | Outcomes and pressure on temporary capacity. |
| `booking_confirmed_total` | Authoritative confirmation rate, distinct from HTTP retries. |
| `capacity_conflict_total`, `idempotency_conflict_total` | Contention/business conflicts and changed-payload reuse. Separate insufficient capacity, duplicate replay and transient DB failure reasons. |
| `outbox_backlog`, oldest pending age, attempt/error counts | Pending work, stalled delivery and recovery progress. |
| `confirmation_latency` and create latency histograms | End-to-end p50/p95/p99, with DB lock wait and commit duration separated. |
| Expiry lag, active overdue holds, consumer duplicate count | Missed polling work, capacity temporarily awaiting release and replay activity. |
| Pool wait, connections, WAL/disk growth, transaction errors | The database and connection budget supporting horizontal scale. |

Use bounded dimensions such as operation/outcome. Keep BookingId, VoyageId, HoldId, TraceId and idempotency-key hash in diagnostic logs, not unbounded metric labels. Periodically reconcile reserved/confirmed counters against persisted hold and booking state; any mismatch is an invariant alert, not an automatic repair instruction.

A later load test should mix hot and cold voyages, success and rejection, concurrent retries, expiration, and broker downtime. Increase offered load in stages while measuring admitted throughput and tail latency. Identify the saturation point and recovery behavior before setting concurrency limits or promising an SLO.

## Recovery and operational limits

Persisted Active deadlines remain discoverable after restart. Expiry checks state under the common lock order and releases capacity once. If workers lag, expired Active holds can temporarily remain in the reserved counter; they cannot pass confirmation's deadline check. A confirmation that waits across its deadline must fail when it finally samples database time.

Outbox defaults are a 30-second lease, one-second base retry delay and one-minute maximum delay. A graceful cancellation or abrupt death can leave a lease; it becomes reclaimable at its database deadline. Recovery time also depends on polling, backlog and lock/connection availability. If a publish completes after its lease is stolen, its old owner cannot mark or reschedule the new owner's row.

The local consumer commits inbox plus projection before transport acknowledgement. A failure before that commit rolls both back; failure after delivery but before outbox marking creates a safe repeat. A future external effect such as email or payment cannot share this inbox transaction automatically; it needs its own idempotent operation or another outbox.

Production RabbitMQ consumers should bound unacknowledged work and avoid immediate requeue loops during outages. Publisher confirmation and consumer acknowledgement cover different links in delivery. These broker mechanisms are proposed, not installed in this submission. [RabbitMQ acknowledgement and prefetch behavior](https://www.rabbitmq.com/docs/confirms).

There is no automatic poison-message quarantine, data-pruning job, broker administration or capacity repair. Add alerting and an audited retry/quarantine procedure before production. Retain inbox and request-idempotency evidence for the documented replay/retry windows; do not delete it merely to reduce table size. Database backup/restore, clock synchronization, replica failover and broker outage procedures need separate operational verification.
