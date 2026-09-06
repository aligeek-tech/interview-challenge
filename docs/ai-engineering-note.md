# AI Engineering Note

[One-page PDF](../output/pdf/AI_Engineering_Note.pdf)

Codex assisted implementation and assessment remediation in the same task. A primary agent coordinated three agents working on domain/expiry consistency, messaging/recovery and independent verification. AI generated or modified source code, migrations, tests, diagrams, runbooks and explanatory notes. The primary agent integrated the work and reviewed agreement between code and artifacts.

Prompts specified business invariants, shared contracts, file ownership, a universal database lock order, post-lock database-time decisions, stable request/message identities and explicit non-goals. Review prompts demanded concrete failure cases and requirement-linked evidence. Agents also reviewed components outside their original implementation assignments.

Verification uses locked dependency restoration, formatting checks, warnings as errors, real PostgreSQL constraints/transactions, independent application hosts and database sessions, controlled overlap, and fresh processes for crash/restart scenarios. Migration tests preserve the original migration hash and exercise populated upgrades. Exact executed results and limitations are recorded in the verification report; generated tests are not accepted as proof merely because they compile or increase a count.

Rejected approaches included unbounded asynchronous expiry tasks and skipping busy work only within the first candidate batch. The former could exhaust finite connections; the latter could keep later voyages hidden behind an entire blocked batch. The implementation uses bounded keyset traversal across polls, a fixed sweep cutoff and the original lock order. Unused generic domain-event dispatch scaffolding was also rejected; concrete lifecycle audit facts and the required integration event express the actual behavior.

One AI defect found during review was that an expiry poll could exhaust its time budget without resolving work while its wrapper still reported successful progress. The corrected worker distinguishes resolved candidates from attempted candidates, and a regression exercises repeated internal timeouts. Review also found and corrected the earlier Production identity-stub exposure and a readiness check that omitted a consumer-required column.

Candidate responsibility remains separate from AI-generated artifact quality. Ali must personally explain the decisions, run the demonstrations, predict failure outcomes and make a meaningful tested change during live review. This note discloses AI assistance and its verification process; it does not claim that candidate-owned validation, personal mastery or the live assessment has already been completed.
