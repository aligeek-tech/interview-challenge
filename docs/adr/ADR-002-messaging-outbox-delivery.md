# ADR-002 — Messaging, Outbox & Delivery Semantics

Status: accepted for this submission. RabbitMQ is the proposed production adapter; the executable uses a durable local transport.

## Context

A committed booking must retain its `BookingConfirmed` event through process or transport failure. Publishing and updating a relational database cannot be assumed to share one atomic commit. The present domain emits one confirmation per logical booking and has no requirement for total ordering across bookings or a historical streaming platform.

## Decision

Consume the hold, confirm the booking, write audit evidence and insert the outbox event in one PostgreSQL transaction. Only `BookingConfirmed` crosses the integration boundary; hold lifecycle events remain local domain/audit events. The envelope carries a stable `MessageId`, schema version, business identifiers, timestamp and trace identifier.

`OutboxPublisher` atomically claims one due row using `FOR UPDATE SKIP LOCKED`, assigns an expiring lease and increments its attempt count. The claim commits before transport I/O. Failure schedules bounded exponential backoff; process death leaves a reclaimable lease. Success and retry updates require the current lease token. Every retry sends the original MessageId. The publisher releases its source transaction and connection before awaiting transport.

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

Delivery is eventually retried and duplicates are expected. Inbox and idempotency retention must cover promised replay/retry windows. Poison messages currently remain pending with capped retry delay; production requires alerts and a deliberate quarantine/re-drive policy. A slow publish may outlive its lease and be repeated safely; this implementation has no lease heartbeat.

The local transport calls the consumer synchronously and acknowledges **after its durable commit**. It uses no volatile queue. Source and consumer use separate transactions in the same PostgreSQL database for the demonstration. This exercises outbox/inbox crash windows but does not test RabbitMQ protocol behavior, routing, clustering or network partitions. A production transport would acknowledge broker acceptance while the consumer runs independently.

## Revisit conditions

Revisit when replay retention, event ordering, consumer fan-out, measured backlog/latency, database write load or the team's supported broker platform changes. Preserve stable message identity and the local transaction guarantees under any adapter change.
