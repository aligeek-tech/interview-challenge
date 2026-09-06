# Assessment remediation and acceptance evidence

The supplied assessment scored the original submission **88.5/100**, with all seven critical gates passed and personal ownership validation pending. This revision addresses its operational and maintainability findings and strengthens evidence for the already strong concurrency and idempotency rows. It is not an official reassessment or a claim of 100/100. A reviewer must evaluate the revised artifact and the candidate's live performance.

## Findings and implemented changes

| Finding | Implemented response | Reviewable regression evidence |
|---|---|---|
| F1 Unrelated expiry work waits behind a hot voyage | Fixed-cutoff keyset traversal continues across polls; root-level `SKIP LOCKED`, bounded later lock waits, candidate/time limits and per-worker state prevent a blocked first batch from hiding later voyages. Restart safely rescans persisted deadlines. | [ExpiryFairnessTests](../tests/CapacityBooking.IntegrationTests/ExpiryFairnessTests.cs): cold work beyond the original hundred-item batch and an entire poll budget completes before the hot lock releases; independent workers, later booking/hold locks, rollback, cancellation and revisit behavior. |
| F2 Permanent outbox failures retry indefinitely | Classified failures, a persisted per-cycle budget, durable quarantine and idempotent, version-checked operator re-drive. State and delivery audit share transactions; original event identity and payload survive recovery. | [OutboxQuarantineTests](../tests/CapacityBooking.IntegrationTests/OutboxQuarantineTests.cs): permanent/unknown/transient behavior, failure budget, healthy work beside quarantine, stale owners, concurrent operators, replay and atomic rollback. Existing crash/inbox regressions remain relevant. |
| F3 Expiry rules are duplicated outside their aggregate owner | [HoldExpiryTransition](../src/CapacityBooking.Infrastructure/Business/HoldExpiryTransition.cs) invokes the domain root and persists its capacity, hold and audit changes. Lazy expiry, late cancellation and background expiry use this path. Repository operations and request idempotency are focused collaborators. | [ExpiryTransitionTests](../tests/CapacityBooking.IntegrationTests/ExpiryTransitionTests.cs), original domain/deadline tests, real confirmation/expiry races and transaction rollback cases. |
| F4 Health hides schema or worker failure | Bounded version/checksum and runtime-column readiness, independent liveness, monotonic per-process worker activity and backlog/quarantine signals. Empty scans and disabled workers are explicit; caught errors, stalled work and budget exhaustion without resolved work do not masquerade as success. | [HealthReadinessTests](../tests/CapacityBooking.IntegrationTests/HealthReadinessTests.cs): incompatible history, missing runtime columns including consumer receipt time, blocked database/worker, idle success, repeated internal expiry timeouts and recovery. |
| F5 Formatting and service cohesion need polish | Common formatting is part of verification. Business orchestration, transaction-bound repository operations, response projection and durable idempotency have separate responsibilities without generic dispatch scaffolding. | [verify.sh](../scripts/verify.sh) includes formatting verification, locked restore, Release build and tests. Final execution outcomes are recorded in [verification](verification.md). |
| F6 Personal ownership is unverified | Transparent AI disclosure and a code-specific live exercise are retained. This remains pending until the candidate personally explains, changes and tests sensitive behavior. | [AI Engineering Note](ai-engineering-note.md) and [interview walkthrough](interview-walkthrough.md) are preparation and disclosure, not proof that the live requirement has been passed. |

Migration 001 remains unchanged. [Migration 002](../src/CapacityBooking.Infrastructure/Migrations/002_outbox_quarantine.sql) adds recovery state and audit structures and preserves existing publications. [MigrationTests](../tests/CapacityBooking.IntegrationTests/MigrationTests.cs) covers fresh installation, populated upgrade, preservation of original data/history, repeat application and rejection of an altered applied checksum.

## Published rubric and remaining evidence

The original eight challenge weights remain unchanged. Points below reproduce the previous assessment; they are not revised scores. The residuals sum to 11.5 points.

| Criterion | Weight | Previous score out of 4 | Previous points | Remaining points | Evidence offered for reassessment |
|---|---:|---:|---:|---:|---|
| DDD and consistency boundaries | 15 | 3.4 | 12.75 | 2.25 | One owned expiry rule and shared transactional persistence path; explicit repository and coordination boundaries. |
| High-contention concurrency | 20 | 3.8 | 19.00 | 1.00 | Original twenty-session and deadline proofs retained; bounded cross-voyage progress, independent workers and lock/cancellation recovery added. |
| Idempotency and duplicate handling | 15 | 3.8 | 14.25 | 0.75 | [ConcurrentCommandTests](../tests/CapacityBooking.IntegrationTests/ConcurrentCommandTests.cs) adds genuine two-host confirmation, conflicting payload, aborted-owner takeover and committed-rejection replay cases. |
| Outbox, messaging and recovery | 15 | 3.4 | 12.75 | 2.25 | Classified recovery, quarantine and audited operator actions, preserving lease fencing and one effective consumer result. |
| Event Storming and domain discovery | 10 | 3.6 | 9.00 | 1.00 | Model, policies and code agree on expiry ownership and operational recovery; all required categories and seven hotspots remain represented. |
| Architecture and scale judgment | 10 | 3.4 | 8.50 | 1.50 | Concrete worker bounds, fair traversal, recovery policies and operational signals; all requested scale concerns remain addressed without an unmeasured throughput claim. |
| .NET, persistence and testing | 10 | 3.4 | 8.50 | 1.50 | Cohesion, formatter gate, immutable migration history, upgrade tests, meaningful health checks and independent database/process regressions. |
| AI-assisted engineering | 5 | 3.0 | 3.75 | 1.25 | Accurate contribution/verification disclosure and real rejected approaches/corrected defects. Candidate-owned explanation and a meaningful tested change remain pending. |
| Total | 100 | — | 88.50 | 11.50 | Final scoring belongs to the reviewer. |

The assessment did not identify a specific failing behavior behind the residual 0.2 scores in concurrency and idempotency. Stronger evidence supports reassessment; additional test counts alone do not establish a 4/4 score. Resolving one finding may support several related criteria, but it does not predetermine their scores.

## Evidence discipline and scope

The assessment's eight reviewer-added tests were separate from submitted coverage. Six exercised safeguards; two deliberately characterized undesirable expiry blocking and indefinite poison retries. A green characterization test proves that the limitation exists. The revised acceptance conditions require cold-voyage progress and controlled quarantine instead. Reviewer-only tests are not presented as candidate-submitted tests or personally executed candidate work.

The original sensitive tests remain in the suite. New assertions inspect committed records, actual database locks and fresh processes; sequential stale-state checks do not replace genuine concurrency. Final counts, commands, environment and source hashes belong to the [verification report](verification.md), which distinguishes execution from design reasoning.

The submission retains three required diagrams, exactly two ADRs, the scale/security/payment notes and a one-page AI Engineering Note. The existing ADRs describe the revised decisions and consequences. No real broker cluster, payment implementation, authentication platform, UI, Kubernetes or 5,000-request/second benchmark is introduced. The [recovery runbook](recovery-runbook.md) explicitly separates implemented unchanged-payload re-drive from a future controlled repair facility.

## Acceptance before resubmission

1. Every F1–F5 change has a reviewable implementation and meaningful regression; the original critical behavior remains protected.
2. Locked restore, formatting verification, Release build and complete tests pass from clean source, with no unexplained skips. Fresh and populated databases migrate correctly without rewriting 001.
3. Diagram sources/renderings, both ADRs, run instructions and operational limits match the final implementation. The AI note remains within one rendered page.
4. An independent reviewer uses the same published weights and seven gates. No score increase is claimed merely for adding infrastructure or tests.
5. The candidate personally explains the code, predicts altered failure schedules, makes a meaningful sensitive change and verifies it with real PostgreSQL tests. Record actual actions and limitations; do not replace this step with generated answers or an authorship assertion.

This revision can strengthen the submitted artifact. Hiring approval, personal mastery and a final score remain separate judgments.
