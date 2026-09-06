-- Applied migration 001 is immutable. Existing publications remain publications after upgrade.
ALTER TABLE outbox
    ADD COLUMN delivery_state text NOT NULL DEFAULT 'Pending',
    ADD COLUMN failure_count integer NOT NULL DEFAULT 0,
    ADD COLUMN state_version bigint NOT NULL DEFAULT 0,
    ADD COLUMN quarantined_at timestamptz,
    ADD COLUMN quarantine_reason_code text;

UPDATE outbox SET delivery_state='Published' WHERE published_at IS NOT NULL;

ALTER TABLE outbox
    ADD CONSTRAINT outbox_delivery_state_valid CHECK (delivery_state IN ('Pending','Published','Quarantined')),
    ADD CONSTRAINT outbox_failure_count_valid CHECK (failure_count >= 0),
    ADD CONSTRAINT outbox_state_version_valid CHECK (state_version >= 0),
    ADD CONSTRAINT outbox_publication_state_matches CHECK ((delivery_state='Published') = (published_at IS NOT NULL)),
    ADD CONSTRAINT outbox_quarantine_state_matches CHECK (
        (delivery_state='Quarantined' AND quarantined_at IS NOT NULL AND quarantine_reason_code IS NOT NULL)
        OR (delivery_state<>'Quarantined' AND quarantined_at IS NULL AND quarantine_reason_code IS NULL)),
    ADD CONSTRAINT outbox_terminal_state_has_no_lease CHECK (delivery_state='Pending' OR lease_token IS NULL),
    ADD CONSTRAINT outbox_quarantine_reason_bounded CHECK (quarantine_reason_code IS NULL OR length(quarantine_reason_code) BETWEEN 1 AND 64);

DROP INDEX pending_outbox;
CREATE INDEX pending_outbox ON outbox(next_attempt_at,occurred_at,message_id)
    WHERE delivery_state='Pending';
CREATE INDEX quarantined_outbox ON outbox(quarantined_at,message_id)
    WHERE delivery_state='Quarantined';

-- Administrative action IDs are global. The fingerprint protects the complete operator intent.
-- A placeholder is private to its open transaction and cannot commit without its stable result.
CREATE TABLE outbox_admin_requests (
    action_id uuid PRIMARY KEY,
    fingerprint text NOT NULL CHECK (length(fingerprint)=64),
    result_json text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION require_completed_outbox_admin_request() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM outbox_admin_requests WHERE action_id=NEW.action_id AND result_json IS NULL) THEN
        RAISE EXCEPTION 'outbox administration result must commit with its claim' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER outbox_admin_request_must_complete
    AFTER INSERT OR UPDATE ON outbox_admin_requests
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_completed_outbox_admin_request();

-- Retained claim history distinguishes publication attempts from observed failure-budget usage.
CREATE TABLE outbox_delivery_audit (
    audit_id bigserial PRIMARY KEY,
    message_id uuid NOT NULL REFERENCES outbox(message_id),
    action text NOT NULL CHECK (action IN ('Claimed','RetryScheduled','Quarantined','Published','Redriven','StaleOutcomeIgnored')),
    attempt integer NOT NULL CHECK (attempt >= 0),
    failure_count integer NOT NULL CHECK (failure_count >= 0),
    state_version bigint NOT NULL CHECK (state_version >= 0),
    lease_token uuid,
    action_id uuid REFERENCES outbox_admin_requests(action_id),
    actor text NOT NULL CHECK (length(actor) BETWEEN 1 AND 128),
    failure_kind text CHECK (failure_kind IN ('Transient','Permanent','Unknown')),
    reason_code text CHECK (reason_code IS NULL OR length(reason_code) BETWEEN 1 AND 64),
    reason text CHECK (reason IS NULL OR length(reason) BETWEEN 1 AND 512),
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX outbox_delivery_audit_by_message ON outbox_delivery_audit(message_id,audit_id);
CREATE UNIQUE INDEX outbox_delivery_audit_redrive_action ON outbox_delivery_audit(action_id)
    WHERE action='Redriven';
