# Verification and requirement evidence

The submission is verified through real PostgreSQL integration tests, domain tests, and fresh application processes. This report distinguishes executed checks from architectural reasoning and future production work. Machine-readable results are in [verification-results.json](verification-results.json).

Latest complete verification on 6 September 2026: **59 passed, 0 failed, 0 skipped** — 49 integration tests and 10 domain tests. A separate clean source directory restored locked dependencies and built with **0 warnings and 0 errors** before running the entire suite. Tested source hashes are recorded in the JSON report. The README smoke sequence passed, all three Mermaid diagrams rendered and were visually inspected, and NuGet reported no known vulnerable direct or transitive packages at verification time.

## Reproduce the checks

Start the isolated database using the README, set `TEST_DATABASE_URL`, and run `./scripts/verify.sh`. The script performs locked restore, a complete build and both test projects. Every integration fixture creates a unique `capacity_booking_test_...` database, migrates it, and drops that database on completion. Independent HTTP hosts share that fixture database. No in-memory database or SQLite substitute is used.

```sh
# Twenty actual concurrent database sessions contending for the final unit
./scripts/dotnet.sh test tests/CapacityBooking.IntegrationTests --no-build --no-restore \
  --filter FullyQualifiedName~TwentyIndependentDatabaseSessions

# Both race outcomes and a request waiting across the expiry deadline
./scripts/dotnet.sh test tests/CapacityBooking.IntegrationTests --no-build --no-restore \
  --filter FullyQualifiedName~ConcurrencyTests

# True OS-process exit, restart, natural deadline/lease recovery and startup guards
./scripts/dotnet.sh test tests/CapacityBooking.IntegrationTests --no-build --no-restore \
  --filter FullyQualifiedName~ProcessRecoveryTests

# Publisher/consumer transaction failures, duplicates and stale lease ownership
./scripts/dotnet.sh test tests/CapacityBooking.IntegrationTests --no-build --no-restore \
  --filter FullyQualifiedName~RecoveryTests
```

The 20-way test holds the voyage row in a coordinator transaction. Twenty distinct booking/key requests through two application hosts must appear simultaneously in `pg_stat_activity` as active database lock waiters before the coordinator releases the lock. Assertions require one 201, nineteen 409s, one effective hold and reconciled capacity totals. This is real overlapping database work, not twenty sequential stale-version checks.

The confirmation/expiry tests gate the actual application transaction before commit or before the clock read, observe the competing database session waiting, then release it. One test admits confirmation before the deadline and commits after it; one lets expiry commit first; another begins confirmation before expiry but keeps it waiting until after the deadline. Domain tests cover exact equality and equivalent instants with different offsets. The 1.5-second test decision window is controlled by gates and database-clock polling; a slow machine that cannot admit the intended pre-deadline decision fails visibly rather than silently changing the expected outcome.

The process tests launch separate OS processes. One creates a two-second hold, terminates, waits for its original persisted deadline during downtime, then starts a new process to expire it. Another exits with code 86 immediately after delivery and before publication marking; recovery uses its original short test lease, the same MessageId and one inbox/projection result. No scheduling metadata is rewritten in these process demonstrations. Focused lease-unit scenarios separately set up synthetic past lease state to exercise specific ownership conditions.

## Eleven required tests

| Requirement | Representative executable evidence |
|---|---|
| Closed voyage rejects a hold | `BusinessLifecycleTests.ClosedVoyageRejectsHoldWithoutReservingCapacity` |
| Capacity cannot be oversold | `DatabaseConstraintRejectsOversellingEvenOutsideApplicationCode`, domain overflow checks and persisted accounting assertions |
| Twenty competing requests, one unit | `ConcurrencyTests.TwentyIndependentDatabaseSessionsCompetingForOneUnitHaveExactlyOneWinner` |
| Expired hold cannot confirm | `BusinessLifecycleTests.ExpiredHoldCannotConfirmAndExpiryNeverReleasesTwice` |
| Deterministic confirm/expire race | Three deadline/race tests in `ConcurrencyTests`, plus exact-boundary domain tests |
| Duplicate create, one effective hold | `ConcurrentDuplicateCreatesWaitOnThePersistedClaimAcrossHosts` and original-response/deadline replay test |
| Same key, different payload rejected | `IdempotencyTests.SameKeyWithDifferentPayloadIsAConflict` |
| Publication failure retains event | `FailedPublicationLeavesConfirmationCommittedAndEventAvailableForRetry` and transport-outage process restart |
| Publish before mark is duplicate-safe | `FailureAfterPublishBeforeMarkRedeliversSameMessageWithOneDownstreamEffect` and actual process death at that point |
| Duplicate event, one downstream result | `ConcurrentDuplicateDeliveriesWaitOnInboxAndProduceOneEffect` and inbox/projection rollback test |
| Restart retains expiry work | `ProcessRecoveryTests.FreshOperatingSystemProcessExpiresHoldsPersistedBeforeShutdown` |

## Business-rule and artifact coverage

| Business rule / deliverable | Implementation and additional evidence |
|---|---|
| Capacity invariant and final-unit contention | `VoyageCapacity`, locked row updates, `capacity_not_oversold` check, concurrent test and accounting reconciliation |
| At most one active hold per booking/voyage | Partial unique index; same-key and different-key concurrent tests; direct SQL bypass attempt rejected |
| Hold consumed at most once | Guarded Active transition, unique booking confirmation hold, duplicate confirmation test |
| Expired/cancelled hold cannot confirm | `HoldDeadline`, domain state rules, lazy/worker expiry, cancelled-hold test |
| Confirmed logical request cannot consume another hold | Immutable booking binding, booking uniqueness, confirmed-booking and replacement-hold tests |
| Confirm/expire deterministic | Shared deadline predicate; post-lock DB clock; same universal lock order; real overlap tests |
| Retried commands cause no duplicate effects | Transactional idempotency claim, canonical fingerprint and response; different-host replay; before-commit process exit |
| State and integration event atomic | BookingService confirmation transaction; two injected rollback checkpoints; process exit before commit |
| Broker/process failure does not lose committed event | Persisted outbox, retry schedule and recoverable lease; outage/restart and after-publish crash tests |
| Duplicate delivery causes one effective result | Consumer inbox plus projection transaction; concurrent duplicate and rollback tests; stale publisher success/failure cases |
| Auditable and observable transitions | Atomic `audit_transitions`, structured scoped logs, trace/business identifiers and DB health; sensitive keys hashed |
| Event Storming and DDD | All requested categories and seven hotspot resolutions; two roots, hold entity and value objects; three diagrams |
| Architecture and evolution | Exactly two ADRs; seven-dimension broker comparison; scale note; security and future-payment discussion |
| AI-assisted engineering | One-page AI note with actual agent roles, verification, rejected scaffolding and corrected identity-host defect |
| Reproducible submission | Version-pinned SDK/packages, checksum migrations, documented commands, clean-source verification and archive |

The database-invariant tests directly attempt invalid counter totals, duplicate active holds, another booking's confirmation hold and an unfinished idempotency commit. Expected PostgreSQL constraint failures show that these protections are durable database rules rather than only application-side checks.

## Corrections and evidence limits

Independent review found that the initial host allowed the demonstration identity stub in Production. The host now refuses HTTP serving outside Development or Testing. Two process tests verify that restriction and that failure injection is rejected outside Development. Malformed/null bodies return client errors, and an oversized body is tested against the actual Kestrel server.

No real Kafka/RabbitMQ broker, broker failover, network partition, 5,000-request/second benchmark or external payment integration is claimed. The implemented local transport makes the outbox/inbox crash windows reproducible, but its acknowledgement follows consumer commit; a real broker adapter has separate producer and consumer acknowledgement boundaries. Metrics names and production operating policies are documented proposals, not an installed telemetry stack. Candidate-owned execution and live interview defense remain to be performed by the candidate.
