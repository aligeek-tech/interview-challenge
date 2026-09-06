# Recovery runbook

This runbook covers the implemented expiry worker, outbox quarantine, health probes and local operator commands. Run commands from the repository root after the [README setup](../README.md), using the intended database connection and a built Release application. Operator commands exit without starting the HTTP host or background workers. They do not apply migrations automatically.

## Interpret health before intervening

```sh
curl -sS http://127.0.0.1:5080/health/live
curl -sS http://127.0.0.1:5080/health/ready
```

| Signal | Meaning and response |
|---|---|
| `/health/live` returns 200 | The HTTP process responds. This does not establish database compatibility or worker progress. |
| `/health/ready`, or its `/health` alias, returns healthy with 200 | The expected migration versions/checksums and required runtime columns are available; enabled workers have recent successful polling or durable item progress. |
| Readiness returns degraded with 200 | Quarantine exists or pending/overdue work exceeds the configured warning age. Durable commands remain available while schema and workers are ready. Inspect the queue and dependency; backlog alone is not a reason to restart the process. |
| Readiness returns 503 | Inspect `database.code` and worker states. Causes include incompatible/missing schema, a bounded database timeout, or a starting, failing, stalled or stopped required worker. |
| Workers are disabled | This is explicit configuration, not evidence that expiry or publication is executing elsewhere. Arrange the intended worker topology before relying on automatic recovery. |

The default database probe deadline is two seconds, worker freshness is thirty seconds, and backlog warning age is two minutes. Successful empty scans keep idle workers healthy. Caught failures do not refresh successful activity. An expiry poll that exhausts its budget without resolving a candidate is a failure. Progress age uses monotonic process time; hold deadlines and publication leases use PostgreSQL time.

The response includes pending/quarantined counts, oldest pending age, overdue Active holds and oldest expiry lag. Worker state describes this process. The probe checks known runtime columns and migration compatibility; it is not a detector for every possible manual schema alteration. Production metric export and alert delivery remain separate work.

For schema mismatch, use the reviewed `--migrate` command from the README. Migration 001 is immutable; migration 002 upgrades existing pending and published events. A checksum disagreement requires investigation, not editing migration history to make the probe green.

## Expiry recovery

An Active hold's persisted deadline remains the source of due work. The worker scans a fixed cutoff using an `(expires_at, hold_id)` cursor retained across polls. Busy roots are skipped; later booking/hold lock waits have a local timeout. Every mutation still locks voyage, booking and hold in that order and samples database time afterward.

Defaults bound each poll to 64 candidates per page, 256 examined candidates, 100 successful expirations and a one-second time budget, with a fifty-millisecond lock-wait timeout. The cursor advances beyond blocked batches and wraps to revisit skipped items. Restart discards only the traversal position and safely rescans durable work. Inspect competing transactions and expiry lag before increasing limits; more parallel tasks do not remove a hot voyage's lock.

Do not manually subtract reserved capacity or delete overdue holds. Background expiry, lazy expiry during commands and late cancellation share the same domain transition, persistence and audit path. If a transaction is interrupted, rollback leaves the item discoverable. A committed transition cannot release capacity again.

## Outbox states and retry policy

| State | Automatic behavior |
|---|---|
| Pending | A publisher can claim a due row whose lease is absent or expired. |
| Published | Delivery was acknowledged and the publication outcome committed. The row remains as evidence. |
| Quarantined | Automatic claims stop. The event, failure information and audit history remain available for investigation and explicit re-drive. |

Known permanent validation or downstream identity failures quarantine immediately. Transient and unknown delivery failures use capped exponential backoff. Defaults are a one-second base delay, a one-minute maximum delay and **twenty observed failures per recovery cycle**. Exhausting that budget quarantines the event even when an outage is transient.

`attempts` records lifetime claims. `failure_count` records observed delivery failures in the current cycle. Process death, cancellation and lease takeover do not themselves spend that failure budget. A database failure while committing an outcome/audit also does not classify the event as poison: its existing lease remains recoverable. The default lease lasts thirty seconds; actual recovery also depends on polling, locks and backlog.

Claims and their audit rows commit together. Outcomes and their audit rows commit together in a separate transaction. The lease token prevents an old publisher from marking, retrying or quarantining a newer owner's state. A stale result is recorded as `StaleOutcomeIgnored`. An ambiguous publication can be delivered again with the original MessageId; inbox plus projection commit together to protect one effective downstream result.

## Inspect quarantine

```sh
./scripts/dotnet.sh run --project src/CapacityBooking.Api -c Release --no-build -- \
  --outbox-list --limit 20
```

The list limit defaults to 100 and accepts 1–1000. Output contains message/event/aggregate identity, lifetime attempts, cycle failure count, quarantine time, reason code and `stateVersion`; it does not dump payloads or credentials. Inspect the corresponding `outbox_delivery_audit` records through authorized database access when detailed attempt history is needed.

Determine whether the cause is an unavailable dependency, a compatibility/configuration defect, invalid stored data or a downstream identity conflict. Restore the dependency or deploy the reviewed compatible code first. A re-drive cannot repair an invalid payload or an inconsistent downstream record.

## Re-drive a resolved failure

Set `MESSAGE_ID` and `STATE_VERSION` from the inspected quarantine item. Generate one new UUID for `ACTION_ID`, and set `OPERATOR_NAME` and `RECOVERY_REASON` to the operator identity and concrete recovery reason. Retain those exact values if the command's outcome is uncertain.

```sh
./scripts/dotnet.sh run --project src/CapacityBooking.Api -c Release --no-build -- \
  --outbox-redrive \
  --message-id "$MESSAGE_ID" \
  --version "$STATE_VERSION" \
  --action-id "$ACTION_ID" \
  --actor "$OPERATOR_NAME" \
  --reason "$RECOVERY_REASON"
```

Actor and reason must be nonempty, contain no control characters, and fit 128 and 512 characters respectively. Actor is **self-reported audit metadata**. Authorization comes from controlled operating-system and database access; this CLI does not authenticate that name or expose an administrative HTTP endpoint.

A successful action changes Quarantined to Pending, increments the state version, clears quarantine/lease/error fields, resets the cycle failure count and schedules the event now. It preserves MessageId, payload, lifetime attempts and earlier audit records. Its state change, audit record and completed administrative result commit atomically.

`ActionId` identifies the complete operator intent, including message, expected version, actor and reason. Repeating the same action replays its saved result, even if publication has subsequently finished. Changing that intent under the same ActionId returns `ActionConflict`. A different action using an obsolete state version returns `VersionConflict`. These safeguards prevent concurrent operators from resetting a newer recovery cycle accidentally.

| Exit code | Interpretation |
|---|---|
| 0 | Listing succeeded, or the re-drive action succeeded/replayed successfully. A successful re-drive does not mean publication has finished. |
| 2 | Invalid arguments or administrative request. |
| 3 | State/version/action conflict; inspect the current item and the returned status. |
| 4 | Message not found. |

Unexpected host/database failures also terminate unsuccessfully. If the outcome is uncertain, retry the same complete ActionId request. Do not invent a new action merely to bypass an unresolved result. A genuinely new reviewed attempt needs a newly inspected version and a new ActionId.

After re-drive, verify publication state and delivery audit history. If it quarantines again, investigate the new reason instead of repeatedly resetting its budget. Confirm the original MessageId still has at most one effective downstream projection.

## Invalid payloads and controlled repair boundary

The implemented command re-drives **unchanged payloads only**. There is no payload editor or automatic source/projection repair. Fixing compatible code or configuration is preferable when the original event is valid.

If persisted event data is actually corrupt, a separately reviewed manual repair procedure is required:

1. Keep the event quarantined. Preserve its original payload and envelope in restricted evidence, and record its MessageId, state version and SHA256 payload hash. Inspect confirmed booking/hold records and committed business audit facts; do not reconstruct business truth from an unverified log line.
2. Establish the authoritative intended event and investigate any existing inbox/projection result. Re-drive of an already consumed MessageId will not amend that projection. Do not create a new MessageId to evade duplicate protection.
3. Obtain the environment owner's approved repair plan. The repair transaction must lock the exact outbox row, require Quarantined state and the reviewed version/hash, and recheck the immutable booking/message identity before changing data.
4. Record before/after hashes, operator, reason and reference to the authoritative evidence atomically with the repair; increment the state version and leave the item quarantined. The current delivery audit action set has no payload-repair action, so this requires a separately reviewed repair tool/migration and audit design. Do not disguise a payload edit as a normal `Redriven` action.
5. Independently validate the repaired event, then inspect its new version and use the implemented re-drive command with a new ActionId. Verify the resulting delivery and downstream state.

This describes the controls required for a manual repair; the repository does not implement that repair facility. Never edit applied migration files, erase inbox/idempotency records or alter capacity counters as a recovery shortcut.

Relevant executable evidence is in [expiry fairness tests](../tests/CapacityBooking.IntegrationTests/ExpiryFairnessTests.cs), [quarantine tests](../tests/CapacityBooking.IntegrationTests/OutboxQuarantineTests.cs), [process recovery tests](../tests/CapacityBooking.IntegrationTests/ProcessRecoveryTests.cs), [health tests](../tests/CapacityBooking.IntegrationTests/HealthReadinessTests.cs) and [migration tests](../tests/CapacityBooking.IntegrationTests/MigrationTests.cs). See [verification](verification.md) for the final executed results.
