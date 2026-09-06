# ADR-002 — Messaging, Outbox & Delivery Semantics

Status: accepted for this submission. RabbitMQ is the proposed production adapter; the executable uses a durable local transport.

## Context

A committed booking must retain its `BookingConfirmed` event through process or transport failure. Publishing and updating a relational database cannot be assumed to share one atomic commit. The present domain emits one confirmation per logical booking and has no requirement for total ordering across bookings or a historical streaming platform.

## Decision

Consume the hold, confirm the booking, write audit evidence and insert the outbox event in one PostgreSQL transaction. Only `BookingConfirmed` crosses the integration boundary; hold lifecycle events remain local domain/audit events. The envelope carries a stable `MessageId`, schema version, business identifiers, timestamp and trace identifier.

`OutboxPublisher` atomically claims one due row using `FOR UPDATE SKIP LOCKED`, assigns an expiring lease and increments its attempt count. The claim commits before transport I/O. Classified permanent validation failures quarantine immediately; transient/unknown delivery failures retry with capped exponential delay and a persisted budget (default 20 observed failures per cycle). Exhaustion quarantines. Process death leaves a reclaimable lease without spending the failure budget. Success, retry and quarantine updates require the current lease token and Pending state; each outcome commits with its delivery audit. Database errors while recording outcomes propagate and retain recoverable work. Every retry sends the original MessageId. The publisher releases its source transaction and connection before awaiting transport.

The consumer inserts `(consumer_name, message_id)` into its durable inbox and writes the booking-confirmation projection in one independent transaction. Duplicate receipt returns without repeating the effect. Projection uniqueness additionally protects BookingId and HoldId. This guarantees one effective **database** result under duplicate delivery, not end-to-end exactly-once delivery or exactly-once external effects.

For production, choose RabbitMQ with durable quorum queues, persistent messages, publisher confirms, mandatory-return handling for unroutable publications, and manual consumer acknowledgement after the consumer transaction commits. A publisher confirm concerns broker acceptance; it does not establish consumer completion. [RabbitMQ acknowledgement semantics](https://www.rabbitmq.com/docs/confirms).

| Concern | Reasoning and trade-off |
|---|---|
| Ordering | Independent confirmations need no global order. Multiple publishers/consumers and redelivery must be assumed capable of changing effective order. Add aggregate sequence/version handling if future events become causally dependent. [RabbitMQ queue ordering](https://www.rabbitmq.com/docs/queues#message-ordering). |
| Replay / retention | A quorum queue is delivery work, not our historical event archive. Kafka provides retained events and replay, with ordering within a partition. RabbitMQ Streams also supports repeated reads. Neither replay system is required here. [Kafka event storage](https://kafka.apache.org/41/getting-started/introduction/), [RabbitMQ Streams](https://www.rabbitmq.com/docs/streams). |
| Routing | Propose a durable topic exchange, a versioned booking-confirmation routing key and a queue per consuming capability. Exchange bindings provide explicit routing; instances of one capability share its queue. [RabbitMQ exchanges](https://www.rabbitmq.com/docs/exchanges#topic). |
| Consumer model | Bounded concurrent consumers, manual acknowledgement and inbox protection fit this small downstream workflow. The consumer database remains its own transaction boundary. |
| Throughput | There is no measured broker throughput claim. Measure confirmation-event rate, payload size, publisher-confirm latency and consumer commit cost before sizing. Hold-request rate is not event-publication rate. |
| Failure recovery | Outbox retry covers producer uncertainty; consumer idempotency covers redelivery. Quorum queues require an available majority; broker deployment still needs explicit resilience and recovery testing. [Quorum queue availability](https://www.rabbitmq.com/docs/quorum-queues#availability). |
| Operational complexity | This choice assumes ownership of queue policies, quorum placement, disk capacity, retry/quarantine handling and monitoring. It is a fit to the workflow, not a claim that RabbitMQ is universally simpler or faster. |

## Alternatives

Kafka is appropriate if retained history, many independently replaying consumers or stream processing becomes central. Its producer/transaction features do not atomically commit this PostgreSQL business state with an external consumer database. Direct publish after commit risks event loss; publish before commit risks publishing a rolled-back outcome. A distributed transaction or a full messaging framework adds unnecessary scope here. [Kafka delivery design](https://kafka.apache.org/41/design/design/#message-delivery-semantics).

## Consequences

Pending delivery is retried automatically within its failure budget and duplicates are expected. Quarantine pauses automatic delivery until an operator resolves the cause and explicitly redrives it. Inbox and idempotency retention must cover promised replay/retry windows. The local redrive command conditionally updates one observed quarantine version, preserves MessageId/payload/lifetime attempts, resets the cycle failure count and records actor/reason/action identity. Repeated action IDs replay the stored result; changed intent conflicts. A prolonged transport outage can therefore require operator recovery after budget exhaustion. This is a deliberate bounded-retry trade-off, not event loss. A slow publish may outlive its lease and be repeated safely; this implementation has no lease heartbeat.

The local transport calls the consumer synchronously and acknowledges **after its durable commit**. It uses no volatile queue. Source and consumer use separate transactions in the same PostgreSQL database for the demonstration. This exercises outbox/inbox crash windows but does not test RabbitMQ protocol behavior, routing, clustering or network partitions. A production transport would acknowledge broker acceptance while the consumer runs independently.

## Revisit conditions

Revisit when replay retention, event ordering, consumer fan-out, measured backlog/latency, database write load or the team's supported broker platform changes. Preserve stable message identity and the local transaction guarantees under any adapter change.
