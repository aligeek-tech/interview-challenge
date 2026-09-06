# Security and payment evolution

## Security design

The executable challenge uses `X-Customer-Id` only to demonstrate ownership checks. A production adapter must derive customer/tenant identity from validated authentication claims, never from this client-controlled header. Authorize create, read, confirm and cancel against the persisted booking owner, including idempotent response replay. Missing or inaccessible holds return 404 without exposing another customer's record.

Limit active held units and active holds per customer, with durable accounting for cross-instance quotas where required. Rate limits and bounded concurrent requests should apply per authenticated customer and, when useful, per voyage. Reject overload with bounded retry guidance. Short server-controlled TTLs, immutable booking identity and cancellation help constrain inventory hoarding; idempotency alone does not stop an attacker creating fresh booking IDs.

Confirmation replay cannot consume capacity again: scoped idempotency, booking uniqueness and terminal hold transitions protect the effect. Idempotency keys are not authorization credentials. Apply key length limits and retain deduplication state for the supported retry window. Continue authorizing a caller before returning a previously stored response.

Keep the database on a private network with least-privilege application and separate migration roles. Use parameterized SQL, encrypted transport and managed secrets. The included Compose credentials are development fixtures. The implementation bounds request body and identifier sizes, hashes idempotency keys in logs, and stores error classes rather than exception messages in outbox failures. Production event contracts must exclude customer secrets, access tokens and unnecessary personal information. Keep IDs out of metric labels to avoid unbounded cardinality.

Production authentication, distributed quotas/rate limiting and deployment hardening are design requirements here; they are not claimed as implemented features. The demonstration HTTP host refuses environments other than Development and Testing. The failure-simulation options are accepted only in Development and must remain disabled in deployed environments.

## Future external payment authorization

The local invariant remains in PostgreSQL: capacity transfer, hold consumption, booking confirmation, audit and outbox insertion stay one transaction. Do not keep a voyage lock or database transaction open across a payment-provider call.

A durable process manager becomes appropriate when payment authorization must complete before booking confirmation. Persist the process identity, current step, provider operation ID and retry/timeout state. Request payment authorization with a stable provider idempotency key. After a confirmed provider outcome, try the existing local confirmation operation with its own stable key. The hold may expire while authorization is pending; the payment response does not override the deadline rule.

If payment authorization succeeds but hold consumption fails, compensate by voiding/releasing that authorization using a stable compensation key. If a future provider flow captures money, the corresponding action is a refund, which has different business and settlement semantics. A successful provider call followed by a local crash requires querying/reconciling the provider outcome before retrying or compensating. A timeout is an unknown outcome, not proof of failure.

Payment authorization, status lookup, local confirmation, void/refund requests and process-message handling must tolerate duplicates. Persist coordination messages through an outbox and protect incoming messages with an inbox. Compensation may itself fail and needs retry and operator escalation. This is a documented evolution path; no payment workflow or provider integration is implemented in this submission.
