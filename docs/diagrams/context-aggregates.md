# Context, aggregates and consistency boundaries

[Open the rendered diagram at full size](context-aggregates.svg). The Mermaid source below remains editable.

```mermaid
flowchart TB
    client([Customer API client]):::external
    rabbit["RabbitMQ<br/>production design only"]:::external

    subgraph deployment["Deployment: one ASP.NET Core application"]
        subgraph source["Bounded context: Booking Capacity"]
            api["Task-oriented HTTP API"]:::component
            coordinator["BookingService<br/>application transaction coordinator"]:::component
            expiry["ExpiryService + bounded sweep worker"]:::component
            transition["Shared HoldExpiryTransition<br/>domain decision + caller-owned session"]:::component
            ops["OutboxAdministration CLI<br/>quarantine list + audited redrive"]:::component

            subgraph txA["Consistency boundary A: one local business transaction"]
                key["Persisted idempotency claim<br/>and completed API response"]:::storage
                capacity["Aggregate root: VoyageCapacity<br/>total / reserved / confirmed"]:::aggregate
                hold["Owned entity: CapacityHold<br/>Active → Consumed / Expired / Cancelled"]:::entity
                booking["Aggregate root: Booking<br/>immutable logical request + confirmation"]:::aggregate
                audit["Audit transition rows"]:::storage
                outbox["Outbox: BookingConfirmed integration intent"]:::storage
            end
            publisher["OutboxPublisher<br/>separate claim / outcome transactions"]:::component
        end

        transport["LocalMessageTransport<br/>acknowledges after consumer commit"]:::component

        subgraph downstream["Independent downstream consumer boundary: confirmation projection"]
            consumer["BookingConfirmedConsumer"]:::component
            subgraph txB["Consistency boundary B: separate downstream transaction"]
                inbox["Inbox uniqueness<br/>consumer_name + message_id"]:::storage
                projection["booking_confirmations<br/>one effective business result"]:::storage
            end
        end
    end

    postgres[("PostgreSQL<br/>one physical database for the demonstration")]:::database

    client --> api --> coordinator
    coordinator --> key
    coordinator --> capacity
    coordinator --> booking
    capacity -->|owns| hold
    coordinator --> audit
    coordinator --> outbox
    expiry --> transition --> capacity
    coordinator --> transition
    ops -->|separate version-checked state and audit transaction| postgres
    outbox -->|read pending after source commit| publisher
    publisher --> transport --> consumer
    publisher -. future transport adapter .-> rabbit
    consumer --> inbox --> projection

    key -. persisted in .-> postgres
    capacity -. persisted in .-> postgres
    hold -. persisted in .-> postgres
    booking -. persisted in .-> postgres
    audit -. persisted in .-> postgres
    outbox -. persisted in .-> postgres
    inbox -. persisted in .-> postgres
    projection -. persisted in .-> postgres

    classDef component fill:#dbeafe,stroke:#2563eb,color:#111;
    classDef aggregate fill:#fef3c7,stroke:#92400e,color:#111;
    classDef entity fill:#ffedd5,stroke:#c2410c,color:#111;
    classDef storage fill:#f3f4f6,stroke:#4b5563,color:#111;
    classDef external fill:#ede9fe,stroke:#7c3aed,color:#111;
    classDef database fill:#dcfce7,stroke:#15803d,color:#111;
```

Legend: blue = executing component; amber = aggregate root; orange = owned entity; gray = durable supporting state; purple = external participant; green = physical database. Named containers distinguish deployment, bounded context and transaction scope. Solid arrows show calls, ownership or delivery; dotted arrows show physical persistence or a future adapter.

The diagram shows confirmation's full transaction scope. Create and cancellation use the relevant subset, while expiry has no API idempotency claim. The expiry worker still locks voyage → booking → hold and commits its state/counter/audit changes together. Background polling is scheduling, not durable business ownership.

| Concept | Selected boundary |
|---|---|
| Bounded context | Booking Capacity defines the shared language and invariants for this narrow source capability. The downstream projection demonstrates a consumer that owns independent state. |
| Aggregate | `VoyageCapacity` owns limited-resource accounting and its holds; `Booking` owns the logical request and one confirmation. `CapacityHold` has identity/lifecycle but is an entity inside capacity ownership. |
| Value object | `CapacityUnits` validates positive whole units; `HoldDeadline` defines strict confirmation and inclusive expiry comparisons. |
| Transaction | Boundary A deliberately spans both roots plus the relevant support records. Boundary B atomically commits inbox identity and the downstream projection, on a separate connection/transaction. |
| Repository operations | `BookingPersistenceSession` provides ordered loading, database time and SQL writes. `HoldExpiryTransition` shares the domain expiry decision; `IdempotentCommandExecutor` coordinates durable API replay. Each helper uses the caller-owned transaction. Root counters plus one target/relevant active hold are loaded; historical aggregate collections are not materialized. |
| Integration | `BookingConfirmedMessage` is the versioned contract. Source confirmation is complete before the publisher runs. At-least-once delivery can duplicate this message; Boundary B protects its effective result. |
| Physical storage | Both boundaries use one PostgreSQL database in the demonstration. The downstream tables have no foreign keys to source bookings or outbox, and the consumer does not join source tables. Shared hardware does not make the two commits atomic. |
| Deployment | API, workers and demonstration consumer run in one application. A bounded context, aggregate, table, broker topic/queue and deployment unit are distinct concepts. No separate microservice deployment is needed. |

The coordinator synchronously consumes the hold and confirms the booking; a domain event handler is not used to split that local invariant across transactions. Lifecycle facts are retained in `audit_transitions`. Publication is the first asynchronous reliability boundary, and the publisher never holds a source business transaction open while delivering a message.

The accepted trade-off is serialization of all mutating operations for a hot voyage. The transaction boundary makes this correctness cost explicit; more application instances cannot remove it. A future external payment step belongs to a durable distributed process outside Boundary A, which must remain short and local.
