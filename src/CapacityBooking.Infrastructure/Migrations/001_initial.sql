CREATE TABLE voyage_capacity (
    voyage_id text PRIMARY KEY CHECK (length(voyage_id) BETWEEN 1 AND 128),
    total integer NOT NULL CHECK (total >= 0),
    reserved integer NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    confirmed integer NOT NULL DEFAULT 0 CHECK (confirmed >= 0),
    is_open boolean NOT NULL DEFAULT true,
    CONSTRAINT capacity_not_oversold CHECK (reserved::bigint + confirmed::bigint <= total::bigint)
);

CREATE TABLE bookings (
    booking_id text PRIMARY KEY CHECK (length(booking_id) BETWEEN 1 AND 128),
    voyage_id text NOT NULL REFERENCES voyage_capacity(voyage_id),
    customer_id text NOT NULL CHECK (length(customer_id) BETWEEN 1 AND 128),
    quantity integer NOT NULL CHECK (quantity > 0),
    confirmed_hold_id uuid UNIQUE,
    confirmed_at timestamptz,
    UNIQUE (booking_id, voyage_id),
    CHECK ((confirmed_hold_id IS NULL) = (confirmed_at IS NULL))
);

CREATE TABLE capacity_holds (
    hold_id uuid PRIMARY KEY,
    booking_id text NOT NULL,
    voyage_id text NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    state text NOT NULL CHECK (state IN ('Active','Consumed','Expired','Cancelled')),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    completed_at timestamptz,
    FOREIGN KEY (booking_id,voyage_id) REFERENCES bookings(booking_id,voyage_id),
    UNIQUE (hold_id,booking_id,voyage_id),
    CHECK (expires_at > created_at),
    CHECK ((state='Active') = (completed_at IS NULL))
);
CREATE UNIQUE INDEX one_active_hold_per_booking_voyage
    ON capacity_holds(booking_id,voyage_id) WHERE state='Active';
CREATE INDEX due_capacity_holds ON capacity_holds(expires_at,hold_id) WHERE state='Active';
ALTER TABLE bookings ADD CONSTRAINT booking_confirmed_hold_belongs_to_booking
    FOREIGN KEY (confirmed_hold_id,booking_id,voyage_id)
    REFERENCES capacity_holds(hold_id,booking_id,voyage_id);

CREATE TABLE idempotency_records (
    customer_id text NOT NULL,
    operation text NOT NULL,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    fingerprint text NOT NULL,
    status_code integer,
    response_json text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (customer_id,operation,idempotency_key),
    CHECK ((status_code IS NULL) = (response_json IS NULL))
);

CREATE TABLE outbox (
    message_id uuid PRIMARY KEY,
    event_type text NOT NULL,
    aggregate_id text NOT NULL,
    payload text NOT NULL,
    occurred_at timestamptz NOT NULL,
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_token uuid,
    lease_until timestamptz,
    last_error text,
    UNIQUE (event_type,aggregate_id),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);
CREATE INDEX pending_outbox ON outbox(next_attempt_at,occurred_at,message_id) WHERE published_at IS NULL;

CREATE TABLE inbox (
    consumer_name text NOT NULL,
    message_id uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_name,message_id)
);

-- Independent downstream state: no foreign keys into the source transaction boundary.
CREATE TABLE booking_confirmations (
    booking_id text PRIMARY KEY,
    voyage_id text NOT NULL,
    hold_id uuid NOT NULL UNIQUE,
    quantity integer NOT NULL CHECK (quantity > 0),
    message_id uuid NOT NULL UNIQUE,
    confirmed_at timestamptz NOT NULL
);

CREATE TABLE audit_transitions (
    audit_id bigserial PRIMARY KEY,
    booking_id text NOT NULL,
    voyage_id text NOT NULL,
    hold_id uuid,
    transition text NOT NULL,
    occurred_at timestamptz NOT NULL,
    trace_id text NOT NULL,
    details text NOT NULL
);
CREATE INDEX audit_by_booking ON audit_transitions(booking_id,audit_id);

-- Even an accidental early commit of an idempotency placeholder must fail.
CREATE FUNCTION require_completed_idempotency() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM idempotency_records
               WHERE customer_id=NEW.customer_id AND operation=NEW.operation
                 AND idempotency_key=NEW.idempotency_key
                 AND (status_code IS NULL OR response_json IS NULL)) THEN
        RAISE EXCEPTION 'idempotency response must commit with its claim' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER idempotency_must_complete
    AFTER INSERT OR UPDATE ON idempotency_records
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_completed_idempotency();
