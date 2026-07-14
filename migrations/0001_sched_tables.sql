-- Scheduler tables. The scheduler owns only sched_*; 'schemas', 'datasets' and 'chunks' are
-- shared infrastructure, created IF NOT EXISTS. A pre-schema 'chunks' table must be recreated
-- (WIP; no upgrade path).

CREATE TABLE IF NOT EXISTS datasets (
    id   SMALLSERIAL PRIMARY KEY,
    name TEXT        NOT NULL UNIQUE
);

-- A dataset's schema (tables, fields, default fields) as jsonb, deduped per dataset by content
-- hash. The dataset's current read schema is the row with superseded_at IS NULL; older rows are
-- kept because chunks stay pinned to the schema they were written under.
CREATE TABLE IF NOT EXISTS schemas (
    id            SERIAL      PRIMARY KEY,
    dataset_id    SMALLINT    NOT NULL REFERENCES datasets(id),
    hash          BYTEA       NOT NULL,  -- SHA-256 of the canonical schema json
    schema        JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    superseded_at TIMESTAMPTZ,           -- NULL = the dataset's current read schema
    UNIQUE (dataset_id, hash),
    UNIQUE (id, dataset_id)  -- FK target for the chunks same-dataset schema pin
);

-- At most one current (non-superseded) schema per dataset.
CREATE UNIQUE INDEX IF NOT EXISTS schemas_one_current_per_dataset
    ON schemas (dataset_id) WHERE superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS chunks (
    chunk_pk             BIGSERIAL    PRIMARY KEY,
    dataset_id           SMALLINT     NOT NULL REFERENCES datasets(id),
    chunk_id             TEXT         NOT NULL,
    size                 INT          NOT NULL,
    -- Schema the chunk was written under (the file set derives from its tables). Stamped at
    -- insert with the dataset's current schema when the writer doesn't supply one.
    schema_id            INTEGER      NOT NULL,
    -- Which of schema_id's tables the chunk carries: bit i = table i in sorted-name order,
    -- 0 = the file is legitimately absent. NULL = unknown → all present.
    tables_present       BIT VARYING,
    first_block          BIGINT       NOT NULL,
    -- last_block - first_block; a chunk's span fits in INT, so store the delta not a second BIGINT.
    last_block_delta     INT          NOT NULL,
    last_block_hash      TEXT,
    last_block_timestamp BIGINT,
    registered_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (dataset_id, chunk_id),
    -- The pinned schema must belong to the chunk's own dataset. DB-enforced because external
    -- clients stamp schema_id (same reasoning as chunk_corrections_same_dataset).
    CONSTRAINT chunks_schema_same_dataset
        FOREIGN KEY (schema_id, dataset_id) REFERENCES schemas (id, dataset_id)
);

-- The non-overlap probe asks "does any live chunk in this dataset overlap the candidate's range?".
-- A btree can bound only one side of that predicate, and liveness lives in sched_chunk_metadata,
-- invisible to any chunks index -- so btree probes degrade to O(N²) when a dataset arrives whole in
-- one batch (no live row to early-exit on; minutes at ~150K chunks). The GiST index covers both
-- range endpoints, so a probe touches only truly intersecting rows; btree_gist lets dataset_id
-- lead. The probe must build the range with this exact expression to ride the index.
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE INDEX IF NOT EXISTS chunks_dataset_range_gist ON chunks USING gist (
    dataset_id,
    int8range(first_block, first_block + last_block_delta, '[]')
);

-- One row per scheduling cycle.
CREATE TABLE IF NOT EXISTS sched_worker_assignments (
    id                SERIAL PRIMARY KEY,
    created_at        BIGINT NOT NULL,
    scheduler_version TEXT
);

-- One row per visibility cycle. created_at anchors the M-tick drain window; confirmed_up_to is
-- the worker-assignment confirmation watermark the cycle saw (drain derivation: see
-- sched_stale_mappings).
CREATE TABLE IF NOT EXISTS sched_portal_assignments (
    id               SERIAL PRIMARY KEY,
    created_at       BIGINT NOT NULL,
    confirmed_up_to  INT    NOT NULL
);

CREATE TABLE IF NOT EXISTS sched_workers (
    id             SERIAL      PRIMARY KEY,
    peer_id        TEXT        NOT NULL UNIQUE,
    version        TEXT,               -- semver e.g. '2.8.0'; NULL = unknown
    inactive_since BIGINT              -- NULL = currently active
);

-- Highest worker assignment confirmed by workers.
CREATE TABLE IF NOT EXISTS sched_worker_confirmations (
    assignment_id  INT    PRIMARY KEY REFERENCES sched_worker_assignments(id),
    confirmed_at   BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS sched_chunk_metadata (
    chunk_pk                        BIGINT PRIMARY KEY REFERENCES chunks(chunk_pk),
    applied_at_worker_assignment_id INT    REFERENCES sched_worker_assignments(id),
    applied_at_portal_assignment_id INT    REFERENCES sched_portal_assignments(id),
    marked_for_removal              BIGINT,  -- a tick, not an id: stays 64-bit
    -- True if the chunk was rejected at registration for overlapping a live chunk in its dataset;
    -- such a chunk is never scheduled or reconsidered. (A bool, not a tick: registration has no
    -- clock.)
    rejected                        BOOLEAN NOT NULL DEFAULT FALSE,
    dropped_at_portal_assignment_id INT    REFERENCES sched_portal_assignments(id),
    -- Tick at which the chunk was tombstoned (pulled from the worker layer), m_ticks after its
    -- portal drop. NULL = not tombstoned.
    dropped_from_worker_assignment_at BIGINT
);

-- What the scheduler currently wants; published worker assignment = this ∪ sched_stale_mappings.
-- No chunks FK, same as the twin below: the cycle re-stages the whole ideal via COPY, and an FK
-- would cost a per-row check on every staged row. Integrity comes from sched_worker_assignment_diffs
-- instead — a pk new to the ideal always differs from the live ideal, so the same cycle transaction
-- diffs it into that table, whose chunks FK validates it once per pk. The parent side never fires;
-- chunks rows are never deleted (sched_chunk_metadata's FK pins them).
CREATE TABLE IF NOT EXISTS sched_ideal_chunk_workers (
    chunk_pk    BIGINT PRIMARY KEY,
    worker_ids  INT[]  NOT NULL
);

-- Twin of sched_ideal_chunk_workers. run_scheduling_cycle stages the freshly computed ideal here,
-- derives the per-cycle stale/diff/flip-flop deltas by set-joining the live ideal against it
-- (server-side, no app-side diffing), then rename-swaps the two tables. Structure must stay identical
-- to sched_ideal_chunk_workers: after a swap this table *is* the old ideal (and vice versa), so both
-- need the same bare PK to remain a valid drop-in.
CREATE TABLE IF NOT EXISTS sched_future_ideal_chunk_workers (
    chunk_pk    BIGINT PRIMARY KEY,
    worker_ids  INT[]  NOT NULL
);

-- What portals route by; lags the ideal until the confirmation watermark advances.
CREATE TABLE IF NOT EXISTS sched_confirmed_chunk_workers (
    chunk_pk    BIGINT PRIMARY KEY, -- deliberately do not reference chunks
    worker_ids  INT[]  NOT NULL
);

-- Per-cycle routing deltas waiting to be replayed into sched_confirmed_chunk_workers.
CREATE TABLE IF NOT EXISTS sched_worker_assignment_diffs (
    worker_assignment_id  INT    NOT NULL REFERENCES sched_worker_assignments(id),
    chunk_pk              BIGINT NOT NULL REFERENCES chunks(chunk_pk),
    worker_ids            INT[]  NOT NULL,  -- empty array = remove from confirmed routing
    PRIMARY KEY (worker_assignment_id, chunk_pk)
);

-- Grace-period holdovers for (chunk, worker) pairs removed from the ideal; draining (not stored)
-- once a portal assignment's confirmed_up_to covers superseded_at_worker_assignment_id.
CREATE TABLE IF NOT EXISTS sched_stale_mappings (
    chunk_pk                            BIGINT NOT NULL REFERENCES chunks(chunk_pk),
    worker_id                           INT    NOT NULL REFERENCES sched_workers(id),
    superseded_at_worker_assignment_id  INT    NOT NULL REFERENCES sched_worker_assignments(id),
    PRIMARY KEY (chunk_pk, worker_id)
);

CREATE INDEX IF NOT EXISTS sched_stale_mappings_worker_id
    ON sched_stale_mappings (worker_id);

-- Serves the expiry delete's watermark-cutoff range.
CREATE INDEX IF NOT EXISTS sched_stale_mappings_superseded
    ON sched_stale_mappings (superseded_at_worker_assignment_id);

-- Reshuffles mint and expire millions of stale rows at once; reclaim dead tuples promptly.
ALTER TABLE sched_stale_mappings SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_cost_delay = 0
);

CREATE INDEX IF NOT EXISTS sched_chunk_metadata_portal_drop
    ON sched_chunk_metadata (dropped_at_portal_assignment_id)
    WHERE dropped_at_portal_assignment_id IS NOT NULL
      AND dropped_from_worker_assignment_at IS NULL;

-- drop_marked_chunks flips every marked-but-not-yet-dropped row each visibility cycle; the
-- predicate matches the UPDATE's, so an idle cycle is an empty index scan instead of a full
-- metadata scan. Rows leave the index when their portal drop lands.
CREATE INDEX IF NOT EXISTS sched_chunk_metadata_marked
    ON sched_chunk_metadata (chunk_pk)
    WHERE marked_for_removal IS NOT NULL
      AND dropped_at_portal_assignment_id IS NULL;

-- The tombstoned-stale cleanup deletes by "tombstoned at tick $now"; indexing the tick keeps that
-- lookup off a full metadata scan (tombstoned rows accumulate for the table's lifetime).
CREATE INDEX IF NOT EXISTS sched_chunk_metadata_tombstoned
    ON sched_chunk_metadata (dropped_from_worker_assignment_at)
    WHERE dropped_from_worker_assignment_at IS NOT NULL;

-- Serves the promotion gate (visibility cycle): the small frontier of chunks confirmed by a worker
-- assignment but not yet portal-visible. `NOT rejected` is the non-obvious term — rejected rows never
-- get an applied_at_worker_assignment_id so the query already skips them, but they satisfy the other
-- predicates, and without it they'd sit in the index as dead NULL-key entries.
CREATE INDEX IF NOT EXISTS sched_chunk_metadata_promotable
    ON sched_chunk_metadata (applied_at_worker_assignment_id)
    WHERE NOT rejected
      AND applied_at_portal_assignment_id IS NULL
      AND dropped_at_portal_assignment_id IS NULL
      AND marked_for_removal IS NULL;

-- The scheduling cycle stamps applied_at_worker_assignment_id onto chunks new to the ideal. The
-- stamp is first-touch (never reset), so the unapplied set is the new-chunk frontier plus the
-- never-placed/rejected residue — without this index the stamp joins the full future ideal
-- against metadata every cycle to change a few thousand rows.
CREATE INDEX IF NOT EXISTS sched_chunk_metadata_unapplied
    ON sched_chunk_metadata (chunk_pk)
    WHERE applied_at_worker_assignment_id IS NULL;

-- 1-to-1 chunk swap mechanism; applied_at_portal_assignment_id NULL = pending.
CREATE TABLE chunk_corrections (
    old_chunk_pk                    BIGINT   PRIMARY KEY REFERENCES chunks(chunk_pk),
    new_chunk_pk                    BIGINT   NOT NULL    REFERENCES chunks(chunk_pk),
    dataset_id                      SMALLINT NOT NULL    REFERENCES datasets(id),
    created_at                      BIGINT   NOT NULL,  -- a tick, not an id: stays 64-bit
    applied_at_portal_assignment_id INT      REFERENCES sched_portal_assignments(id),
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
