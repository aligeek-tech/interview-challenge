# Verification and requirement evidence

The submission is verified through real PostgreSQL integration tests, domain tests, and fresh application processes. This report distinguishes executed checks from architectural reasoning and future production work. Machine-readable results are in [verification-results.json](verification-results.json).

Revision 2 verification on 6 September 2026: **119 passed, 0 failed, 0 skipped** - 109 cases in the integration-test project and 10 domain tests. The integration project includes a focused worker-health state check as well as real PostgreSQL, HTTP and process scenarios. The original 59 cases remain in the suite; this revision adds 60. Both the repository verification script and a separate clean source copy passed locked restore, formatting verification, Release build with **0 warnings and 0 errors**, and the complete suite. Runtime/build/test source hashes match the verified copy and are recorded in the JSON report. Actual Kestrel API and operator CLI smoke checks passed; all three diagrams and the one-page AI-note PDF were rendered and visually inspected. NuGet reported no known vulnerable direct or transitive packages at verification time.

## Reproduce the checks

Start the isolated database using the README, set `TEST_DATABASE_URL`, and run `./scripts/verify.sh`. The script performs locked restore, formatting verification, a Release build and both test projects. Every integration fixture creates a unique `capacity_booking_test_...` database, migrates it, and drops that database on completion. Independent HTTP hosts share that fixture database. No in-memory database or SQLite substitute is used.

```sh
# Twenty actual concurrent database sessions contending for the final unit
./scripts/dotnet.sh test tests/CapacityBooking.IntegrationTests -c Release --no-build --no-restore \
  --filter FullyQualifiedName~TwentyIndependentDatabaseSessions

# Both race outcomes and a request waiting across the expiry deadline
./scripts/dotnet.sh test tests/CapacityBooking.IntegrationTests -c Release --no-build --no-restore \
  --filter FullyQualifiedName~ConcurrencyTests

# True OS-process exit, restart, natural deadline/lease recovery and startup guards
./scripts/dotnet.sh test tests/CapacityBooking.IntegrationTests -c Release --no-build --no-restore \
  --filter FullyQualifiedName~ProcessRecoveryTests

# Publisher/consumer transaction failures, duplicates and stale lease ownership
./scripts/dotnet.sh test tests/CapacityBooking.IntegrationTests -c Release --no-build --no-restore \
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
| Auditable and observable transitions | Atomic business/delivery audit, structured scoped logs, trace/business identifiers, bounded schema/worker readiness and backlog visibility; sensitive keys hashed |
| Event Storming and DDD | All requested categories and seven hotspot resolutions; two roots, hold entity and value objects; three diagrams |
| Architecture and evolution | Exactly two ADRs; seven-dimension broker comparison; scale note; security and future-payment discussion |
| AI-assisted engineering | One-page AI note with actual agent roles, verification, rejected scaffolding and corrected identity-host defect |
| Reproducible submission | Version-pinned SDK/packages, checksum migrations, documented commands, clean-source verification and archive |

The database-invariant tests directly attempt invalid counter totals, duplicate active holds, another booking's confirmation hold and an unfinished idempotency commit. Expected PostgreSQL constraint failures show that these protections are durable database rules rather than only application-side checks.

## Corrections and evidence limits

Independent review found that the initial host allowed the demonstration identity stub in Production. The host now refuses HTTP serving outside Development or Testing. Two process tests verify that restriction and that failure injection is rejected outside Development. Malformed/null bodies return client errors, and an oversized body is tested against the actual Kestrel server.

No real Kafka/RabbitMQ broker, broker failover, network partition, 5,000-request/second benchmark or external payment integration is claimed. The implemented local transport makes the outbox/inbox crash windows reproducible, but its acknowledgement follows consumer commit; a real broker adapter has separate producer and consumer acknowledgement boundaries. Health readiness, worker progress and backlog/quarantine signals are implemented. Exported metrics, alert delivery and wider production operating policies remain proposals rather than an installed telemetry stack. Candidate-owned execution and live interview defense remain to be performed by the candidate.

## Revision 2 regression and recovery evidence

| Risk | Executed evidence |
|---|---|
| Entire old batch hides later voyages | `ExpiryFairnessTests.ColdVoyageBeyondTheOriginalHundredAndAnEntirePollBudgetExpiresBeforeHotLockIsReleased`: 129 older hot holds, page size 4, poll budget 8, scope recreation, cold completion before releasing the hot root. |
| Tail locks, two workers, cancellation and restart | Separate booking/hold blockers, same-voyage unaffected work, independent worker sessions, bounded busy scans, interrupted-write rollback and fresh sweep recovery in `ExpiryFairnessTests`. |
| Duplicated expiry behaviour | Four-trigger state/counter/audit parity and shared-write rollback in `ExpiryTransitionTests`; original deadline/lifecycle tests preserved. |
| Concurrent command replay | Same/different-key confirmations, payload conflict, aborted-owner takeover and committed rejection replay across two hosts in `ConcurrentCommandTests`. |
| Poison and retry exhaustion | Permanent validation cases, transient/unknown persisted budget, healthy work alongside poison and fresh-process quarantine isolation in `OutboxQuarantineTests`. |
| Recovery action ambiguity | Same/different concurrent actions, stale versions, stable rejected-result replay, old lease outcomes, claim/outcome/admin audit rollback and deferred administrative completion constraint in `OutboxQuarantineTests`. |
| Misleading health | Incompatible history, required columns, bounded blocked database, actual worker stalls, repeated unresolved internal expiry budgets, idle success, degradation and recovery in `HealthReadinessTests`; monotonic productive-batch state in `WorkerHealthTests`. |
| Existing-database upgrade | Fresh install, populated 001-to-002 upgrade retaining source data/history and Published/Pending states, repeated migration, and altered-checksum rejection in `MigrationTests`. |

The test command list and all 119 individual outcomes/durations are retained in the JSON report. Test durations characterize this verification run, not application throughput. All integration fixtures use randomly named disposable databases; the clean copy contained no prebuilt artifacts.

The separate smoke run used actual Kestrel processes and the executable CLI against one disposable database. It verified create 201, identical create replay 201, confirm 200, GET Consumed, cancellation 409, Location/trace headers and counters reserved=0/confirmed=99/total=100. A configured transport outage exhausted a one-failure test budget. CLI list/redrive/replayed action then recovered the unchanged message under a healthy process, with one redrive audit and one downstream effect. MessageId and payload remained unchanged. The smoke database and processes were removed afterward.

See [assessment remediation](assessment-remediation.md) for rubric/finding mapping and [recovery runbook](recovery-runbook.md) for operator commands, policy trade-offs and the unimplemented payload-repair boundary. Candidate-owned validation remains pending; the new tests and smoke run were performed by the AI-assisted workflow.
