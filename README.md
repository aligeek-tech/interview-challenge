# Expiring Capacity Hold and Booking Confirmation

A focused .NET backend for temporary voyage-capacity reservations. PostgreSQL protects the capacity invariant, logical booking identity and duplicate requests. Confirmation commits together with its integration event. Durable expiry and outbox workers recover from process loss; a transactional inbox protects one downstream projection from duplicate delivery.

The implementation is one deployable application. It uses .NET 10, ASP.NET Core, Dapper, Npgsql and PostgreSQL. Explicit SQL makes the database behavior visible. The included transport is a synchronous local demonstration adapter: it acknowledges after the downstream transaction commits. The production RabbitMQ adapter is a documented design, not an implemented broker connection.

## Run from a clean checkout

Prerequisites: the .NET SDK pinned by `global.json` (10.0.400, with servicing patch roll-forward), Docker with Compose, and a shell. PostgreSQL 18.6 is provided by Compose. An existing PostgreSQL server can be used instead; the integration-test role must be allowed to create databases. The application role does not require that permission in production.

All commands run from the repository root. `scripts/dotnet.sh` uses a repository-local SDK if present and otherwise uses `dotnet` on PATH. The local SDK is not part of the submission.

```sh
docker compose -p capacity-booking-challenge up -d --wait db
export ConnectionStrings__Database='Host=127.0.0.1;Port=55439;Database=capacity_booking;Username=challenge;Password=challenge_dev;Maximum Pool Size=64;Timeout=5;Command Timeout=15'
export ASPNETCORE_ENVIRONMENT=Development
./scripts/dotnet.sh restore CapacityBooking.sln --locked-mode
./scripts/dotnet.sh build CapacityBooking.sln -c Release --no-restore
./scripts/dotnet.sh run --project src/CapacityBooking.Api -c Release --no-build -- --migrate
./scripts/dotnet.sh run --project src/CapacityBooking.Api -c Release --no-build -- --seed
./scripts/dotnet.sh run --project src/CapacityBooking.Api -c Release --no-build --urls http://127.0.0.1:5080
```

The demo credentials are local-only fixtures, and the database port binds to loopback. The seed creates `VYG-1001` with 100 units and confirms a 97-unit baseline through the application service, leaving three units for the scenario. `VYG-LAST` has one unit, and `VYG-CLOSED` rejects new holds. A successfully completed seed is repeatable. An interrupted seed may leave a temporary baseline hold; let it expire before retrying. Seeding is intended for a fresh demonstration database.

`--migrate` applies embedded, ordered SQL files in one transaction under a PostgreSQL advisory lock. Applied migration checksums prevent silently editing migration history. Application startup does not automatically change the schema. `/health` is the readiness alias: it checks a bounded database connection, the exact migration manifest/checksums, required runtime columns, and enabled worker health. It does not apply migrations. Upgrade existing databases with `--migrate`; migration 001 remains unchanged.

If port 55439 is occupied, set `POSTGRES_PORT` before Compose startup and use that port in both connection strings. Stop only this project's database with `docker compose -p capacity-booking-challenge stop db`. Its named volume preserves state.

## Exercise the API

```sh
curl -i http://127.0.0.1:5080/api/voyages/VYG-1001/capacity-holds \
  -H 'X-Customer-Id: demo-customer' \
  -H 'Idempotency-Key: demo-create-9001' \
  -H 'Content-Type: application/json' \
  -d '{"bookingId":"BKG-9001","quantity":2}'
```

Use the returned `holdId` for confirmation, GET and DELETE. Complete examples are in [requests.http](requests.http).

| Method and path | Behavior |
|---|---|
| `POST /api/voyages/{voyageId}/capacity-holds` | Reserve units; 201 with hold and deadline |
| `POST /api/capacity-holds/{holdId}/confirm` | Consume active eligible hold; 200 with confirmed booking |
| `DELETE /api/capacity-holds/{holdId}` | Cancel and release once; 200 for repeated cancellation; 409 if consumed |
| `GET /api/capacity-holds/{holdId}` | 200 with effective current state, or 404 |
| `GET /health` or `/health/ready` | 200 when schema and required workers are ready; 503 otherwise. Delivery degradation is visible in the JSON response. |
| `GET /health/live` | 200 while the HTTP process responds, independently of database/worker readiness. |

The two POST operations require one `Idempotency-Key` of 1–128 visible ASCII characters. API errors contain a stable `code` and readable `message`. Invalid input returns 400, missing demonstration identity 401, inaccessible/missing holds 404, business conflicts 409, and transient database failures 503 with retry advice. Responses expose `X-Trace-Id`; original creation responses also provide `Location`.

**Identity boundary:** `X-Customer-Id` is an untrusted demonstration identity stub. Persisted ownership checks prevent one supplied customer identifier from accessing another customer's records, but a client can impersonate another identifier. This is not production authentication. The HTTP host therefore starts only in Development or Testing; schema/seed commands can run without an HTTP host. The production security design is in [security and payment evolution](docs/security-and-payment.md).

## Business and consistency decisions

- Capacity uses positive integer units. `reserved + confirmed <= total` and nonnegative counters are database checks. Domain methods preserve these rules and repository writes execute under the voyage row lock.
- A `BookingId` identifies one immutable customer/voyage/quantity request. A replacement after expiry or cancellation retains this identity. A different request requires a different booking ID.
- One active hold per booking/voyage is protected by a partial unique index. Changing an idempotency key cannot create another active hold or confirm a logical booking twice.
- All business writers lock voyage, then booking, then hold. The idempotency claim precedes those locks where applicable. Hold identity lookups and expiry candidate scans do not lock holds first.
- After acquiring all relevant business locks, the transaction samples `clock_timestamp()`. Confirmation requires `decisionTime < expiresAt`; equality expires. HTTP arrival time and transaction start time confer no priority. A successful pre-deadline decision can commit after the deadline; a rollback discards it.
- Creating a hold increases reserved units. Confirmation transfers them to confirmed units. Cancellation and expiry release reserved units once. Expiry may run repeatedly without changing terminal holds.
- The worker durably reconciles elapsed deadlines using a bounded keyset sweep. Busy voyages and later booking/hold locks cannot monopolize a poll. The worker retains traversal state across scopes, advances beyond blocked batches, and wraps to retry skipped work; restart safely rescans durable deadlines. GET reports a past-deadline active row as effectively expired without writing. Until a worker or mutating operation processes it, its units remain conservatively reserved. No overselling occurs during this lag.
- A closed voyage rejects new holds. Existing valid holds may confirm; closing an existing voyage is outside this API.
- Hold lifecycle outcomes are persisted audit events. `BookingConfirmed` is the integration event. Capacity, hold, booking, relevant audit rows, outbox event and completed idempotent response commit in the same local transaction.

### Idempotency semantics

The durable key is `(customer, operation including route resource, key)`. A typed canonical fingerprint includes customer, command, route and business payload. JSON whitespace/property order does not change the fingerprint. Reusing the same scoped key with different input returns 409. Reusing a key on a different resource is a different scope.

The first request inserts a placeholder in its transaction. Concurrent duplicates wait on the unique index; after the first transaction commits they read a fresh READ COMMITTED snapshot and replay the stored status/body. A deferred database constraint rejects an unfinished record at commit. A crash before commit leaves neither claim nor business mutation.

Completed business rejections are replayed too. Retrying a sold-out request with the same key remains the same rejection even if capacity later returns; a new business attempt uses a new key. Retrying creation does not extend the original TTL or return a replacement hold. Use GET for current state. Already confirmed requests return their existing confirmation without another event.

Records are retained indefinitely in this small challenge. Production retention must cover the supported client retry horizon, with a documented policy for old-key reuse and retained logical-booking uniqueness. Transient database faults roll back and may be retried with the same key; clients should apply bounded backoff rather than automatically retrying business rejection.

## Verification and failure demonstrations

Integration tests create a uniquely named database, apply the actual migrations, and drop only that test database afterward. The test role requires CREATEDB. Tests never truncate the demonstration database or an unrelated database.

```sh
export TEST_DATABASE_URL='Host=127.0.0.1;Port=55439;Database=postgres;Username=challenge;Password=challenge_dev;Maximum Pool Size=64;Timeout=5;Command Timeout=15'
./scripts/verify.sh
```

The script performs locked restore, formatting verification, Release build and all tests. Results are written as TRX files under `artifacts/test-results`. Unit tests cover domain invariants and exact time-boundary behavior. Integration tests use real PostgreSQL, independent connections and application hosts. The 20-request test holds the voyage lock in a coordinator transaction and observes twenty database lock waiters before releasing it. Confirm/expire tests control lock/decision gates instead of depending on short sleeps. Process tests launch a separate application executable against retained database state.

Focused commands and exact test mappings are in [verification evidence](docs/verification.md). The full suite includes all eleven required scenarios and additional checks for business uniqueness with different keys, ownership, double release, rollback atomicity and consumer transaction failure.

Development-only failure controls support OS-process demonstrations. Set `FailureInjection__Point` to `confirm.before-commit`, `outbox.after-publish` or another internal observer point. A match exits the process with code 86 without graceful disposal. Optional `FailureInjection__ResourceId` limits the target, and `FailureInjection__SignalFile` records that the point was reached. `DemoTransport__Unavailable=true` makes publication fail before delivery. These settings are rejected outside Development and have no public HTTP endpoints.

For a publication crash, the local consumer may have committed while the outbox is still leased. Restart without the failpoint: once the lease expires, the publisher redelivers the same MessageId, the inbox recognizes it, and the publisher records success. The implemented guarantee is at-least-once publication and one effective transactional projection result. An external email/payment effect would need its own idempotency or outbox boundary.

## Recovery operations

Permanent message validation failures enter durable quarantine. Transient/unknown observed delivery failures use exponential backoff and a configurable budget (default 20 per delivery cycle); exhaustion also quarantines. Process death, cancellation and lease takeover do not spend this failure budget. A short outage recovers automatically; an exhausted outage requires explicit audited redrive after recovery. Quarantined rows do not block healthy messages.

Use [the recovery runbook](docs/recovery-runbook.md) for list/redrive commands, retry semantics, health interpretation and safe handling of malformed payloads. [Assessment remediation](docs/assessment-remediation.md) maps the review findings to implemented changes and regression evidence.

## Repository guide

| Location | Responsibility |
|---|---|
| `src/CapacityBooking.Domain` | Capacity and booking roots, hold entity, units and deadline rules |
| `src/CapacityBooking.Application` | Task contracts, result/message DTOs and test orchestration seam |
| `src/CapacityBooking.Infrastructure` | Explicit PostgreSQL transactions, migrations, durable workers and consumer |
| `src/CapacityBooking.Api` | Four endpoints, health, identity stub, hosted workers and demonstration controls |
| `tests` | Domain, database, cross-instance, concurrency and process-recovery tests |
| `docs/diagrams` | Three required diagrams with model explanations |
| `docs/adr` | Exactly two architecture decision records |

Read [Event Storming](docs/diagrams/event-storming.md), [context and aggregates](docs/diagrams/context-aggregates.md), [failure sequence](docs/diagrams/failure-sequence.md), [ADR-001](docs/adr/ADR-001-capacity-hold-booking-consistency.md), [ADR-002](docs/adr/ADR-002-messaging-outbox-delivery.md), [scale and operations](docs/scale-and-operations.md), [AI engineering note](docs/ai-engineering-note.md), and [interview walkthrough](docs/interview-walkthrough.md).

The scope excludes payment implementation, customer management, UI, a real broker cluster, Kubernetes, an API gateway, event sourcing and a generic workflow engine. Production deployment would additionally require real authentication, admission controls, operational retention/repair policies and the chosen broker adapter.
