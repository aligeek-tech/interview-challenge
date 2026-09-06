# AI Engineering Note

Codex assisted implementation in one task, with a primary agent coordinating three parallel agents for domain/business transactions, expiry/messaging, and independent verification. The primary agent integrated the schema, API, configuration, scripts and documentation. AI generated or modified the submitted source, SQL migrations, tests, diagrams and explanatory notes.

Prompts specified the business invariants, a shared schema and interface contract, exact file ownership, a universal database lock order, database-time deadline decisions, stable request/message identities, and explicit non-goals. Review prompts asked agents to challenge correctness and map requirements to executable evidence. Separate review work examined components outside the reviewer's original implementation slice.

Verification uses compiler warnings as errors, locked package restoration, real PostgreSQL constraints and transactions, multiple independent sessions/application hosts, controlled concurrency gates, and fresh OS processes for restart and crash scenarios. Exact executed results and commands are recorded in the verification report; generated code is not considered validated merely because it compiles.

One rejected suggestion was to add unused generic domain-event dispatch scaffolding for perceived DDD scoring. The implementation instead persists the concrete lifecycle outcomes and maps the required integration event explicitly. This keeps the event model explainable without adding unused infrastructure.

One AI implementation defect discovered during independent review was that the host initially accepted the demonstration identity header in Production despite its intended development-only scope. The host now refuses serving outside Development or Testing, and startup behavior is covered by verification. This avoids presenting the demonstration header as production authentication. The test/review cycle also checked deadline sampling after lock waits and stale outbox lease ownership rather than assuming those paths were correct from compilation alone.

Candidate responsibility remains separate from generated artifact quality. The candidate must personally run the demonstrations, review the decisions and code, and explain the guarantees and limitations during the technical review. This note describes AI assistance; it does not claim that candidate-owned validation or interview preparation has already occurred.
