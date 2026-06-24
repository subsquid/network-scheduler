-- Scheduler tables. The scheduler owns only sched_*; 'datasets' and 'chunks'
-- are shared infrastructure created IF NOT EXISTS so a fresh database works,
-- while shared deployments that already have them are unaffected.

CREATE TABLE IF NOT EXISTS datasets (
    id      SMALLSERIAL PRIMARY KEY,
    name    TEXT        NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_pk             BIGSERIAL    PRIMARY KEY,
    dataset_id           SMALLINT     NOT NULL REFERENCES datasets(id),
    chunk_id             TEXT         NOT NULL,
    size                 INT          NOT NULL,
    files                TEXT[]       NOT NULL,
    first_block          BIGINT       NOT NULL,
    -- last_block - first_block; a chunk's span fits in INT, so store the delta not a second BIGINT.
    last_block_delta     INT          NOT NULL,
    last_block_hash      TEXT,
    last_block_timestamp BIGINT,
    registered_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (dataset_id, chunk_id)
);

-- The non-overlap checks (registration + promotion) probe "live chunks in this dataset whose range
-- reaches up to the candidate's first block". Indexing the range end (first_block + last_block_delta)
-- makes that probe selective for tip-of-chain ingestion. See docs/nonoverlap-promotion-gate.md.
CREATE INDEX IF NOT EXISTS chunks_dataset_last_block
    ON chunks (dataset_id, (first_block + last_block_delta));

-- One row per scheduling cycle.
CREATE TABLE IF NOT EXISTS sched_worker_assignments (
    id                BIGSERIAL   PRIMARY KEY,
    created_at        BIGINT NOT NULL,
    scheduler_version TEXT
);

-- One row per visibility cycle. created_at anchors the M-tick drain window.
CREATE TABLE IF NOT EXISTS sched_portal_assignments (
    id         BIGSERIAL   PRIMARY KEY,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS sched_workers (
    id             BIGSERIAL   PRIMARY KEY,
    peer_id        TEXT        NOT NULL UNIQUE,
    version        TEXT,               -- semver e.g. '2.8.0'; NULL = unknown
    inactive_since BIGINT              -- NULL = currently active
);

-- Highest worker assignment confirmed by workers.
CREATE TABLE IF NOT EXISTS sched_worker_confirmations (
    assignment_id  BIGINT      PRIMARY KEY REFERENCES sched_worker_assignments(id),
    confirmed_at   BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS sched_chunk_metadata (
    chunk_pk                        BIGINT PRIMARY KEY REFERENCES chunks(chunk_pk),
    applied_at_worker_assignment_id BIGINT REFERENCES sched_worker_assignments(id),
    applied_at_portal_assignment_id BIGINT REFERENCES sched_portal_assignments(id),
    marked_for_removal              BIGINT,
    -- True if the chunk was rejected at registration for overlapping a live chunk in its dataset;
    -- such a chunk is never scheduled or reconsidered. (A bool, not a tick: registration has no
    -- clock.)
    rejected                        BOOLEAN NOT NULL DEFAULT FALSE,
    dropped_at_portal_assignment_id BIGINT REFERENCES sched_portal_assignments(id),
    dropped_at_worker_assignment_id BIGINT REFERENCES sched_worker_assignments(id)
);

-- What the scheduler currently wants; published worker assignment = this ∪ sched_stale_mappings.
CREATE TABLE IF NOT EXISTS sched_ideal_chunk_workers (
    chunk_pk    BIGINT   PRIMARY KEY REFERENCES chunks(chunk_pk),
    worker_ids  BIGINT[] NOT NULL
);

-- What portals route by; lags the ideal until the confirmation watermark advances.
CREATE TABLE IF NOT EXISTS sched_confirmed_chunk_workers (
    chunk_pk    BIGINT   PRIMARY KEY REFERENCES chunks(chunk_pk),
    worker_ids  BIGINT[] NOT NULL
);

-- Per-cycle routing deltas waiting to be replayed into sched_confirmed_chunk_workers.
CREATE TABLE IF NOT EXISTS sched_worker_assignment_diffs (
    worker_assignment_id  BIGINT   NOT NULL REFERENCES sched_worker_assignments(id),
    chunk_pk              BIGINT   NOT NULL REFERENCES chunks(chunk_pk),
    worker_ids            BIGINT[] NOT NULL,  -- empty array = remove from confirmed routing
    PRIMARY KEY (worker_assignment_id, chunk_pk)
);

-- Grace-period holdovers for (chunk, worker) pairs removed from the ideal.
CREATE TABLE IF NOT EXISTS sched_stale_mappings (
    chunk_pk                            BIGINT NOT NULL REFERENCES chunks(chunk_pk),
    worker_id                           BIGINT NOT NULL REFERENCES sched_workers(id),
    superseded_at_worker_assignment_id  BIGINT NOT NULL REFERENCES sched_worker_assignments(id),
    dropped_at_portal_assignment_id     BIGINT REFERENCES sched_portal_assignments(id),
    PRIMARY KEY (chunk_pk, worker_id)
);

CREATE INDEX IF NOT EXISTS sched_stale_mappings_worker_id
    ON sched_stale_mappings (worker_id);

CREATE INDEX IF NOT EXISTS sched_chunk_metadata_portal_drop
    ON sched_chunk_metadata (dropped_at_portal_assignment_id)
    WHERE dropped_at_portal_assignment_id IS NOT NULL
      AND dropped_at_worker_assignment_id IS NULL;

-- Serves the promotion gate (visibility cycle): chunks confirmed by a worker assignment, not yet
-- portal-visible, not dropped, not marked for removal, not rejected. The partial predicate keeps it
-- to the small promotable frontier rather than the whole table. `NOT rejected` excludes
-- registration-rejected rows: they never get an applied_at_worker_assignment_id (so the query already
-- skips them via IS NOT NULL), but they do satisfy the other partial conditions — without this they'd
-- sit in the index as dead NULL-key entries.
CREATE INDEX IF NOT EXISTS sched_chunk_metadata_promotable
    ON sched_chunk_metadata (applied_at_worker_assignment_id)
    WHERE NOT rejected
      AND applied_at_portal_assignment_id IS NULL
      AND dropped_at_portal_assignment_id IS NULL
      AND marked_for_removal IS NULL;

-- 1-to-1 chunk swap mechanism; applied_at_portal_assignment_id NULL = pending.
CREATE TABLE chunk_corrections (
    old_chunk_pk                    BIGINT   PRIMARY KEY REFERENCES chunks(chunk_pk),
    new_chunk_pk                    BIGINT   NOT NULL    REFERENCES chunks(chunk_pk),
    dataset_id                      SMALLINT NOT NULL    REFERENCES datasets(id),
    created_at                      BIGINT   NOT NULL,
    applied_at_portal_assignment_id BIGINT   REFERENCES sched_portal_assignments(id),
    CHECK (old_chunk_pk <> new_chunk_pk)
);

-- The replacement must share the old chunk's dataset; enforced for every client by this trigger.
CREATE FUNCTION chunk_corrections_same_dataset() RETURNS trigger AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM chunks WHERE chunk_pk = NEW.old_chunk_pk AND dataset_id <> NEW.dataset_id)
       OR EXISTS (SELECT 1 FROM chunks WHERE chunk_pk = NEW.new_chunk_pk AND dataset_id <> NEW.dataset_id) THEN
        RAISE EXCEPTION
            'chunk_corrections: old chunk % and new chunk % must both belong to dataset_id %',
            NEW.old_chunk_pk, NEW.new_chunk_pk, NEW.dataset_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER chunk_corrections_same_dataset
    BEFORE INSERT OR UPDATE OF old_chunk_pk, new_chunk_pk, dataset_id ON chunk_corrections
    FOR EACH ROW EXECUTE FUNCTION chunk_corrections_same_dataset();

-- Serves the pending-set scan and the "is this chunk the new_chunk_pk of a pending correction?"
-- membership lookup (i.e. is it still being produced by an earlier chain link).
CREATE INDEX chunk_corrections_pending_by_new_chunk
    ON chunk_corrections (new_chunk_pk)
    WHERE applied_at_portal_assignment_id IS NULL;
